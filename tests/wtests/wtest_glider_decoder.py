import functools
import json
import logging
import pathlib

from cnodc.nodb import NODBObservationData, NODBObservation, NODBQueueItem
from cnodc.nodb.observations import NODBWorkingRecord
from cnodc.processing.workflow.progressor import WorkflowProgressWorker
from cnodc.programs.file_scan import FileScanTask, FileDownloadWorker
from cnodc.programs.glider.workers import GliderConversionWorker, GliderMetadataUploadWorker
from cnodc.programs.nodb import NODBDecodeLoadWorker
from helpers.base_test_case import BaseTestCase
from helpers.mock_workflow import MockWorkflow, WorkerEvent
from helpers.web_mock import MockResponse
from autoinject import injector
import zirconium as zr


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

class TestGliderDecode(BaseTestCase):

    @classmethod
    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def setUpClass(cls):
        super().setUpClass()
        cls.reset_db_before_tests = False
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
        workflow = MockWorkflow(cls.nodb)
        workflow.add_worker(FileScanTask, {
            'run_on_boot': True,
            'scan_target': str(input_dir),
            'workflow_name': 'test_glider_decode',
            'delay_seconds': 600,
            'pattern': '*.nc',
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
            cls.workflow_result = workflow.test_file(
                pathlib.Path(__file__).parent.parent / 'programs' / 'gliders' / 'SEA032_20250606_R.nc',
                input_dir
            )

    def assertEventDidOccur(self, process_name: str, event_name: str, msg: str = None):
        for x in self.workflow_result.worker_events:
            if x.event_name == event_name and x.process_name == process_name:
                return x
        raise self.failureException(msg or f"Event {process_name}:{event_name} not found")


    def assertEventDidNotOccur(self, process_name: str, event_name: str, msg: str = None):
        for x in self.workflow_result.worker_events:
            if x.event_name == event_name and x.process_name == process_name:
                raise self.failureException(msg or f"Event {process_name}:{event_name} found unexpectedly!")

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

    def test_observations(self):
        self.assertEqual(7474, self.db.rows(NODBObservation.TABLE_NAME))

    def test_observation_data(self):
        self.assertEqual(7474, self.db.rows(NODBObservationData.TABLE_NAME))

    def test_no_working_records(self):
        self.assertEqual(0, self.db.rows(NODBWorkingRecord.TABLE_NAME))


