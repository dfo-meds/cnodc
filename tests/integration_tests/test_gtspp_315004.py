import zirconium as zr
from autoinject import injector

import medsutil.ocproc2 as ocproc2
from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_name
from nodb.observations import NODBSourceFile, NODBObservation, NODBObservationData, NODBWorkingRecord, NODBPlatform, \
    PlatformStatus
from medsutil.ocproc2.codecs import GtsCodec
from medsutil.ocproc2 import QCTestRunInfo, QCResult
from pipeman.processing.progressor import WorkflowProgressWorker
from pipeman.programs.file_scan import FileScanTask, FileDownloadWorker
from pipeman.programs.gtspp.bathymetry import GTSPPBathymetryCheck
from pipeman.programs.gtspp.coordinate import GTSPPCoordinateCheck
from pipeman.programs.gtspp.speed import GTSPPSpeedCheck
from pipeman.programs.nodb.loader import NODBDecodeLoadWorker
from pipeman.programs.qc.integrity import NODBIntegrityChecker
from pipeman.programs.qc.platform import NODBPlatformCheck
from pipeman.programs.qc.preflight import NODBPreFlight
from pipeman.programs.qc.qcworker import NODBQCWorker
from tests.helpers.base_test_case import skip_integration_test, ordered_after, load_ordered_tests
from tests.helpers.mock_workflow import BaseWorkflowTestCase, MockWorkflow, WorkflowTestResult


load_tests = load_ordered_tests

@skip_integration_test
class TestGTSPPForValid315004(BaseWorkflowTestCase):

    @classmethod
    @injector.inject
    @injector.test_case
    def build_and_run_workflow(cls, workflow: MockWorkflow, cfg: zr.ApplicationConfig) -> WorkflowTestResult:
        with cls.real_nodb as db:
            platform = NODBPlatform()
            platform.wmo_id = "1200345"
            platform.platform_name = "ShippyMcShip"
            platform.platform_id = "12"
            platform.platform_type = "ship"
            platform.metadata = {
                'skip_speed_check': False,
                'top_speed': 50,
            }
            platform.service_start_date = AwareDateTime(2010, 1, 1, 3, 1, 3, tzinfo="Etc/UTC")
            platform.status = PlatformStatus.ACTIVE
            db.insert_object(platform)
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
            'workflow_name': 'test_gtspp',
            'delay_seconds': 600,
            'pattern': '*.bufr',
            'metadata': {
                'source_name': 'gts',
                'program_name': 'gtspp',
            }
        })
        workflow.add_worker(FileDownloadWorker)
        workflow.add_worker(NODBDecodeLoadWorker,{
            'queue_name': 'decode_records',
            'error_directory': str(error_dir),
        })
        workflow.add_worker(WorkflowProgressWorker)
        workflow.add_worker(NODBQCWorker, {
            'qc_tests': [
                dynamic_name(NODBPreFlight),
                dynamic_name(NODBIntegrityChecker),
            ],
            'queue_name': 'nodb_preflight_check',
            'review_queue': 'nodb_preflight_review',
            'error_queue': 'nodb_preflight_error',
            'escalation_queue': 'nodb_preflight_escalation',
        })
        workflow.add_worker(NODBQCWorker, {
            'qc_tests': [
                dynamic_name(NODBPlatformCheck),  # 1.1
            ],
            'queue_name': 'nodb_station_check',
            'review_queue': 'nodb_station_review',
            'error_queue': 'nodb_station_error',
            'escalation_queue': 'nodb_station_escalation',
        })
        workflow.add_worker(NODBQCWorker, {
            'qc_tests': [
                dynamic_name(GTSPPCoordinateCheck), # 1.2, 1.3
                dynamic_name(GTSPPSpeedCheck), # 1.5

                # need to get the bathymetry files for testing
                #dynamic_name(GTSPPBathymetryTest), # 1.4, 1.6, 2.11
            ],
            'queue_name': 'gtspp_qca_check',
            'review_queue': 'gtspp_qca_review',
            'error_queue': 'gtspp_qca_error',
            'escalation_queue': 'gtspp_qca_escalation',
        })
        workflow.add_workflow(
            'test_gtspp',
            str(ego_dir),
            [
                {
                  'order': 0,
                   'name': 'decode_records',
                   'worker_config': {
                       'decoder': {
                           'decoder_class': dynamic_name(GtsCodec),
                       },
                   }
                },
                {
                    'order': 1,
                    'name': 'nodb_preflight_check',
                },
                {
                    'order': 2,
                    'name': 'nodb_station_check',
                },
                {
                    'order': 3,
                    'name': 'gtspp_qca_check',
                }
            ],
        )
        return workflow.test_file(
            cls.data_file_path('bufr/315004_2.bufr'),
            input_dir
        )

    def test_download_ran(self):
        self.assertEventDidOccur("file_downloader", "before_queue_item")
        self.assertEventDidOccur("file_downloader", "after_queue_item")
        self.assertEventDidOccur("file_downloader", "on_success")
        self.assertEventDidNotOccur("file_downloader", "on_retry")
        self.assertEventDidNotOccur("file_downloader", "on_failure")

    @ordered_after(test_download_ran)
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

    @ordered_after(test_decode_ran)
    def test_source_file(self):
        with self.real_nodb as db:
            source_files = [x for x in NODBSourceFile.find_all(db)]
            self.assertEqual(1, len(source_files))
            self.assertEqual(source_files[0].source_name, 'gts')
            self.assertEqual(source_files[0].program_name, 'gtspp')

    @ordered_after(test_source_file)
    def test_observations(self):
        with self.real_nodb as db:
            self.assertEqual(0, db.rows(NODBObservation.TABLE_NAME))

    @ordered_after(test_observations)
    def test_observation_data(self):
        with self.real_nodb as db:
            self.assertEqual(0, db.rows(NODBObservationData.TABLE_NAME))

    @ordered_after(test_observation_data)
    def test_working_records(self):
        with self.real_nodb as db:
            self.assertEqual(1, db.rows(NODBWorkingRecord.TABLE_NAME))

    @ordered_after(test_working_records)
    def test_qc_run(self):
        self.assertEventDidOccur("qc_worker", "before_queue_item")
        self.assertEventDidOccur("qc_worker", "after_queue_item")
        self.assertEventDidOccur("qc_worker", "on_success")
        self.assertEventDidNotOccur("qc_worker", "on_retry")
        self.assertEventDidNotOccur("qc_worker", "on_failure")

    @ordered_after(test_qc_run)
    def test_working_record(self):
        with self.real_nodb as db:
            wrs = [x for x in NODBWorkingRecord.find_all(db)]
            self.assertEqual(1, len(wrs))
            wr = wrs[0]
            self.assertIsInstance(wr, NODBWorkingRecord)
            record = wr.record
            self.assertIsInstance(record, ocproc2.ParentRecord)

            integrity_test = record.latest_test_result("nodb_integrity")
            self.assertIsInstance(integrity_test, QCTestRunInfo)
            self.assertEqual(integrity_test.test_name, "nodb_integrity")
            self.assertEqual(integrity_test.test_version, "1.0")
            self.assertIs(integrity_test.result, QCResult.PASS)

            platform_test = record.latest_test_result("nodb_platform")
            self.assertIsInstance(platform_test, QCTestRunInfo)
            self.assertEqual(platform_test.test_name, "nodb_platform")
            self.assertEqual(platform_test.test_version, "1.0")
            self.assertIs(platform_test.result, QCResult.PASS)

            self.assertIn("CNODCPlatform", record.metadata)
            self.assertFalse(record.metadata["CNODCPlatform"].is_empty())
            self.assertEqual(1, record.metadata["CNODCPlatform"].quality)
