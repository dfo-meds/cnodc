import datetime

from cnodc.nodb import NODBBatch, BatchStatus, NODBObservation
from cnodc.nodb.observations import NODBWorkingRecord, NODBObservationData
from cnodc.ocproc2 import ParentRecord
from cnodc.processing.workflow.payloads import BatchPayload
from cnodc.programs.nodb import NODBFinalizeWorker
from processing.helpers import WorkerTestCase


class TestFinalizer(WorkerTestCase):

    def test_finalizer(self):
        bp = BatchPayload('12345')
        batch = NODBBatch()
        batch.batch_uuid = '12345'
        batch.status = BatchStatus.NEW
        self.db.insert_object(batch)
        wr = NODBWorkingRecord()
        wr.working_uuid = '123456'
        wr.received_date = '2015-10-12'
        wr.source_file_uuid = '123'
        wr.message_idx = 0
        wr.record_idx = 0
        record = ParentRecord()
        record.coordinates['Time'] = '2015-10-11T00:00:00+00:00'
        record.coordinates['Latitude'] = 34.12
        record.coordinates['Latitude'].metadata['Units'] = 'degrees_north'
        record.coordinates['Longitude'] = -123.12
        record.coordinates['Longitude'].metadata['Units'] = 'degrees_east'
        wr.record = record
        wr.qc_batch_id = '12345'
        self.db.insert_object(wr)
        self.worker_controller.test_queue_worker(
            NODBFinalizeWorker,
            {},
            self.worker_controller.payload_to_queue_item(bp, 'nodb_finalize')
        )
        obs_data: NODBObservationData = NODBObservationData.find_by_source_info(
            self.db,
            source_file_uuid='123',
            source_received_date='2015-10-12',
            message_idx=0,
            record_idx=0
        )
        self.assertIsNotNone(obs_data)
        obs: NODBObservation = obs_data.find_observation(self.db)
        self.assertIsNotNone(obs)
        self.assertEqual(obs.obs_time, datetime.datetime.fromisoformat("2015-10-11T00:00:00+00:00"))
        b: NODBBatch = NODBBatch.find_by_uuid(self.db, '12345')
        self.assertIs(b.status, BatchStatus.COMPLETE)

