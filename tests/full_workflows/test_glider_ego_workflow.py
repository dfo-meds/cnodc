import functools
import logging

from nodb import NODBObservationData, NODBObservation, NODBSourceFile
from nodb import NODBWorkingRecord
from pipeman.processing.progressor import WorkflowProgressWorker
from pipeman.programs.file_scan import FileScanTask, FileDownloadWorker
from pipeman.programs.glider.workers import GliderConversionWorker, GliderMetadataUploadWorker
from pipeman.programs.nodb import NODBDecodeLoadWorker
from tests.helpers.base_test_case import skip_long_test
from tests.helpers.mock_workflow import MockWorkflow, WorkflowTestResult, BaseWorkflowTestCase
from tests.helpers.mock_requests import MockResponse
from autoinject import injector
import zirconium as zr
import medsutil.json as json


def with_security(cb):
    @functools.wraps(cb)
    def _inner(method, url, **kwargs):
        h = kwargs.pop('headers', {})
        if 'Authorization' not in h:
            return MockResponse(b"Forbidden", 403)
        if h['Authorization'] != '12345':
            return MockResponse(b"Forbidden", 403)
        return cb(method, url, **kwargs)
    return _inner

@with_security
def upsert_dataset(method, url, data, **kwargs):
    data.append(kwargs.pop('json'))
    return json.dumps({'guid': '23456'})


@skip_long_test
class TestGliderDecode(BaseWorkflowTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def build_and_run_workflow(cls, workflow: MockWorkflow) -> WorkflowTestResult:
        cls.set_log_level_for_class(logging.ERROR)
        input_dir = cls.class_temp_dir / 'inputs'
        input_dir.mkdir()
        error_dir = cls.class_temp_dir / 'errors'
        error_dir.mkdir()
        glider_dir = cls.class_temp_dir / 'gliders'
        glider_dir.mkdir()
        erddap_dir = cls.class_temp_dir / 'erddap'
        erddap_dir.mkdir()
        ego_dir = cls.class_temp_dir / 'ego'
        ego_dir.mkdir()
        workflow.add_worker(FileScanTask, {
            'run_on_boot': True,
            'scan_target': str(input_dir),
            'workflow_name': 'test_glider_decode',
            'delay_seconds': 600,
            'pattern': '*.nc',
            'metadata': {
                'source_name': 'ego_glider_files',
                'program_name': 'gliders',
            }
        })
        workflow.add_worker(FileDownloadWorker)
        workflow.add_worker(NODBDecodeLoadWorker,{
            'queue_name': 'decode_records',
            'error_directory': str(error_dir),
        })
        workflow.add_worker(WorkflowProgressWorker)
        workflow.add_worker(GliderConversionWorker, {
            'openglider_directory': str(glider_dir),
            'openglider_erddap_directory': str(erddap_dir),
        })
        workflow.add_worker(GliderMetadataUploadWorker)
        workflow.add_workflow(
            'test_glider_decode',
            str(ego_dir),
            [{
                'order': 0,
                'name': 'decode_records',
                'worker_config': {
                    'decoder': {
                        'decoder_class': 'cnodc.ocproc2.codecs.netcdf.NetCDFCommonDecoder',
                        'decoder_kwargs': {
                            'mapping_class': 'cnodc.programs.glider.ego_decode.GliderEGOMapper',
                        },
                        'autocomplete_records': True,
                        'allow_reprocessing': True,
                        'hook_before_record': 'cnodc.programs.glider.workers.add_glider_mission_platform_info',
                    },
                }
            }, 'glider_ego_conversion'],
            'cnodc.programs.glider.ego_convert.validate_ego_glider_file'
        )
        cls._web_test_data = []
        cls.web('http://test/api/upsert-dataset', 'POST')(
            functools.partial(upsert_dataset, data=cls._web_test_data)
        )
        with cls.mock_web_test():
            return workflow.test_file(
                cls.data_file_path('glider_ego/SEA032_20250606_R.nc'),
                input_dir
            )

    def test_download_ran(self):
        self.assertEventDidOccur("file_downloader", "before_queue_item")
        self.assertEventDidOccur("file_downloader", "after_queue_item")
        self.assertEventDidOccur("file_downloader", "on_success")
        self.assertEventDidNotOccur("file_downloader", "on_retry")
        self.assertEventDidNotOccur("file_downloader", "on_failure")

    def test_decode_ran(self):
        self.assertEventDidOccur("decoder", "before_queue_item")
        self.assertEventDidOccur("decoder", "after_queue_item")
        self.assertEventDidOccur("decoder", "on_success")
        self.assertEventDidOccur("decoder", "before_message")
        self.assertEventDidOccur("decoder", "before_record")
        self.assertEventDidOccur("decoder", "after_record")
        self.assertEventDidOccur("decoder", "after_message_success")
        self.assertEventDidNotOccur("decoder", "after_decode_error")
        self.assertEventDidNotOccur("decoder", "on_retry")
        self.assertEventDidNotOccur("decoder", "on_failure")

    def test_conversion_ran(self):
        self.assertEventDidOccur("glider_ego_converter", "before_queue_item")
        self.assertEventDidOccur("glider_ego_converter", "after_queue_item")
        self.assertEventDidOccur("glider_ego_converter", "on_success")
        self.assertEventDidNotOccur("glider_ego_converter", "on_retry")
        self.assertEventDidNotOccur("glider_ego_converter", "on_failure")

    def test_metadata_uploader_ran(self):
        self.assertEventDidOccur("glider_metadata_uploader", "before_queue_item")
        self.assertEventDidOccur("glider_metadata_uploader", "after_queue_item")
        self.assertEventDidOccur("glider_metadata_uploader", "on_success")
        self.assertEventDidNotOccur("glider_metadata_uploader", "on_retry")
        self.assertEventDidNotOccur("glider_metadata_uploader", "on_failure")

    def test_dmd_request_made(self):
        self.assertEqual(1, len(self._web_test_data))

    def test_dmd_content(self):
        data = json.dumps(self._web_test_data[0])
        data_reloaded = json.loads(data)
        with open("./_test_dmd_output.json", "w") as h:
            h.write(json.dumps(data_reloaded))
        with open(self.data_file_path('glider_openglider/metadata.json'), 'r', encoding='utf-8') as h:
            content = h.read()
            content = json.loads(content)
            self.assertDictSimilar(data_reloaded, content)

    def test_observations(self):
        with self.real_nodb as db:
            self.assertEqual(7474, db.rows(NODBObservation.TABLE_NAME))

    def test_observation_data(self):
        with self.real_nodb as db:
            self.assertEqual(7474, db.rows(NODBObservationData.TABLE_NAME))

    def test_source_file(self):
        with self.real_nodb as db:
            source_files = [x for x in NODBSourceFile.find_all(db)]
            self.assertEqual(1, len(source_files))
            self.assertEqual(source_files[0].source_name, 'ego_glider_files')
            self.assertEqual(source_files[0].program_name, 'gliders')

    def test_no_working_records(self):
        with self.real_nodb as db:
            self.assertEqual(0, db.rows(NODBWorkingRecord.TABLE_NAME))


