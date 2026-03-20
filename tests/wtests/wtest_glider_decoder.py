import pathlib

from cnodc.nodb import NODBObservationData, NODBObservation
from cnodc.nodb.observations import NODBWorkingRecord
from cnodc.processing.workflow.progressor import WorkflowProgressWorker
from cnodc.programs.file_scan import FileScanTask, FileDownloadWorker
from cnodc.programs.glider.workers import GliderConversionWorker, GliderMetadataUploadWorker
from cnodc.programs.nodb import NODBDecodeLoadWorker
from helpers.base_test_case import BaseTestCase
from helpers.mock_workflow import MockWorkflow


class TestGliderDecode(BaseTestCase):

    @classmethod
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
        workflow.add_worker(NODBDecodeLoadWorker,{
            'queue_name': 'glider_ego_decode',
            'error_directory': str(error_dir),
            'decoder_class': 'cnodc.ocproc2.codecs.netcdf.NetCDFCommonDecoder',
            'decoder_kwargs': {
                'mapping_class': 'cnodc.programs.glider.ego_decode.GliderEGOMapper',
            },
            'autocomplete_records': True,
            'allow_reprocessing': True,
            'hook_before_record': 'cnodc.programs.glider.workers.add_glider_mission_platform_info',
        })
        workflow.add_worker(GliderConversionWorker, {
            'openglider_directory': str(glider_dir),
            'openglider_erddap_directory': str(erddap_dir),
        })
        workflow.add_worker(FileDownloadWorker)
        workflow.add_worker(GliderMetadataUploadWorker)
        workflow.add_worker(WorkflowProgressWorker)
        workflow.add_workflow(
            'test_glider_decode',
            str(ego_dir),
            ['glider_ego_decode', 'glider_ego_conversion'],
            'cnodc.programs.glider.ego_convert.validate_ego_glider_file'
        )
        cls.workflow_result = workflow.test_file(
            pathlib.Path(__file__).parent.parent / 'programs' / 'gliders' / 'SEA032_20250606_R.nc',
            input_dir
        )

    def test_observations(self):
        self.assertEqual(7474, self.db.rows(NODBObservation.TABLE_NAME))

    def test_observation_data(self):
        self.assertEqual(7474, self.db.rows(NODBObservationData.TABLE_NAME))

    def test_no_working_records(self):
        self.assertEqual(0, self.db.rows(NODBWorkingRecord.TABLE_NAME))


