import datetime
import os

import nodb as nodb
from medsutil.dynamic import dynamic_name
from nodb import NODBQueueItem, NODBSourceFile, SourceFileStatus, NODBObservation, QueueStatus
from nodb import NODBWorkingRecord, NODBObservationData
from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs import OCProc2JsonCodec
from pipeman.exceptions import CNODCError
from pipeman.processing.payloads import FilePayload, SourceFilePayload, BatchPayload, ObservationPayload, WorkflowPayload
from pipeman.programs.nodb import NODBDecodeLoadWorker
from medsutil.exceptions import CodedError
import medsutil.ocproc2 as ocproc2
from tests.helpers.base_test_case import BaseTestCase


class NODBLoaderBadRecordCreation(NODBDecodeLoadWorker):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.res = True

    def on_start(self):
        self.res = self.get_config('_test_result', True)
        super().on_start()

    def _create_nodb_record(self,
                            source_file: nodb.NODBSourceFile,
                            message_idx: int,
                            record_idx: int,
                            record: ocproc2.ParentRecord,
                            make_completed_records) -> bool:
        if self.res is True or self.res is False:
            return self.res
        raise self.res



class TestLoader(BaseTestCase):

    def _make_test_file(self, records):
        file = self.temp_dir / "file.json"
        codec = OCProc2JsonCodec()
        codec.dump(file, records)
        return FilePayload.from_path(str(file), datetime.datetime.now(datetime.timezone.utc))

    def test_bad_err_dir(self):
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {
            'queue_name': 'test',
        })
        with self.assertRaisesCNODCError('NODB-LOAD-1001'):
            loader.on_start()

    def test_bad_err_dir_is_file(self):
        sd = self.temp_dir / 'file.txt'
        sd.touch()
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {
            'queue_name': 'test',
            'error_directory': str(sd)
        })
        with self.assertRaisesCNODCError('NODB-LOAD-1003'):
            loader.on_start()

    def test_bad_err_dir_no_exist(self):
        sd = self.temp_dir / 'subdir'
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {
            'queue_name': 'test',
            'error_directory': str(sd)
        })
        with self.assertRaisesCNODCError('NODB-LOAD-1003'):
            loader.on_start()

    def test_bad_no_queue_name(self):
        sd = self.temp_dir / 'subdir'
        sd.mkdir()
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {
            'error_directory': str(sd),
            'decoder_class': 'cnodc.ocproc2.codecs.ocproc2json.OCProc2JsonCodec'
        })
        with self.assertRaisesCNODCError('QUEUE-WORKER-1000'):
            loader.on_start()

    def test_bad_payload_type_batch(self):
        bp = BatchPayload(batch_uuid='12345')
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {})
        with self.assertRaisesCNODCError('NODB-LOAD-2000'):
            loader._fetch_source_file(bp)

    def test_bad_payload_type_obs(self):
        op = ObservationPayload(obs_uuid='12345', received_date=datetime.date(2015, 1, 2))
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {})
        with self.assertRaisesCNODCError('NODB-LOAD-2000'):
            loader._fetch_source_file(op)

    def test_bad_payload_type_no_source(self):
        sp = SourceFilePayload(source_uuid='123456', received_date=datetime.date(2015, 1, 2))
        loader: NODBDecodeLoadWorker = self.worker_controller.build_test_worker(NODBDecodeLoadWorker, {})
        with self.assertRaisesCNODCError('PAYLOAD-1012'):
            loader._fetch_source_file(sp)

    def test_loader_from_fresh_file(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        fp = self._make_test_file([pr])
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
            },
            self.worker_controller.payload_to_queue_item(fp, 'test_intake')
        )
        self.assertEqual(1, self.db.rows(NODBWorkingRecord.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBQueueItem.TABLE_NAME))

    def test_loader_from_fresh_file_completed(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        fp = self._make_test_file([pr])
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
                'autocomplete_records': True,
            },
            self.worker_controller.payload_to_queue_item(fp, 'test_intake')
        )
        self.assertEqual(1, self.db.rows(NODBObservation.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBObservationData.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBQueueItem.TABLE_NAME))

    def test_loader_from_fresh_file_bad_json_record(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        file = self.temp_dir / 'file.json'
        with open(file, "w") as h:
            h.write('[')
            h.write('{"_metadata": {"foo": "bar2"}},')
            h.write('{malformed json},')
            h.write('{"_metadata": {"foo": "bar3"}},')
            h.write('{"_metadata": {"foo": "bar"}}')
            h.write(']')
        fp = FilePayload.from_path(str(file), datetime.datetime.now(datetime.timezone.utc))
        with self.assertLogs("cnodc.worker.decoder", "ERROR"):
            self.worker_controller.test_queue_worker(
                NODBDecodeLoadWorker,
                {
                    'queue_name': 'test_intake',
                    'decoder_class': dynamic_name(OCProc2JsonCodec),
                    'error_directory': str(err_dir),
                },
                self.worker_controller.payload_to_queue_item(fp, 'test_intake')
            )
        self.assertEqual(3, self.db.rows(NODBWorkingRecord.TABLE_NAME))
        self.assertEqual(2, self.db.rows(NODBQueueItem.TABLE_NAME))
        self.assertEqual(2, self.db.rows(NODBSourceFile.TABLE_NAME))
        files = [x.path for x in os.scandir(err_dir) if x.name.endswith(".bin")]
        self.assertEqual(1, len(files))
        with open(files[0], "r") as h:
            self.assertEqual("{malformed json}", h.read())
        file1: NODBSourceFile = self.db.tables[NODBSourceFile.TABLE_NAME][0]
        file2: NODBSourceFile = self.db.tables[NODBSourceFile.TABLE_NAME][1]
        self.assertEqual(file2.original_uuid, file1.source_uuid)
        self.assertEqual(file2.received_date, file1.received_date)
        self.assertEqual(file2.original_idx, 1)
        self.assertEqual(file2.source_path, files[0].replace("\\", "/"))
        self.assertEqual(file2.file_name, file1.file_name)
        self.assertIsNotNone(file2.history)

    def test_loader_from_source_file(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.NEW
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
            },
            self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        )
        self.assertEqual(1, len(self.db.tables[NODBWorkingRecord.TABLE_NAME]))
        self.assertEqual(1, len(self.db.tables[NODBQueueItem.TABLE_NAME]))
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNotNone(obj)

    def test_already_completed_source_file(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.COMPLETE
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
            },
            self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        )
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNone(obj)

    def test_already_completed_source_but_reprocessing(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.COMPLETE
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
                'allow_reprocessing': True
            },
            self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        )
        self.assertEqual(1, self.db.rows(NODBWorkingRecord.TABLE_NAME))
        self.assertEqual(1, self.db.rows(NODBQueueItem.TABLE_NAME))
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNotNone(obj)

    def test_errored_source_file(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.ERROR
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
            },
            self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        )
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNone(obj)
        self.assertEqual(1, len(self.db.table(NODBSourceFile.TABLE_NAME)))

    def test_raise_recoverable_error_during_save(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.NEW
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        qi = self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        qi.status = QueueStatus.LOCKED
        self.db.insert_object(qi)
        with self.assertLogs('cnodc.worker.decoder', 'ERROR'):
            self.worker_controller.test_queue_worker(
                NODBLoaderBadRecordCreation,
                {
                    'queue_name': 'test_intake',
                    'decoder_class': dynamic_name(OCProc2JsonCodec),
                    'error_directory': str(err_dir),
                    '_test_result': CNODCError('foo', 'bar', 1, is_transient=True)
                },
                qi
            )
        self.assertIs(qi.status, QueueStatus.UNLOCKED)
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNone(obj)
        self.assertEqual(1, len(self.db.table(NODBSourceFile.TABLE_NAME)))

    def test_raise_unrecoverable_error_during_save(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.NEW
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        qi = self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        qi.status = QueueStatus.LOCKED
        self.db.insert_object(qi)
        with self.assertLogs('cnodc.worker.decoder', 'ERROR'):
            self.worker_controller.test_queue_worker(
                NODBLoaderBadRecordCreation,
                {
                    'queue_name': 'test_intake',
                    'decoder_class': dynamic_name(OCProc2JsonCodec),
                    'error_directory': str(err_dir),
                    '_test_result': CNODCError('foo', 'bar', 1, is_transient=False)
                },
                qi
            )
        self.assertIs(qi.status, QueueStatus.COMPLETE)
        qi2 = self.db.table(NODBQueueItem.TABLE_NAME)[-1]
        self.assertEqual(qi2.queue_name, 'decode_failure')
        pl = WorkflowPayload.from_queue_item(qi2)
        self.assertIsInstance(pl, SourceFilePayload)
        self.assertEqual(pl.source_uuid, '12345')
        self.assertEqual(pl.received_date, datetime.date(2015, 1, 2))
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNone(obj)
        self.assertEqual(1, len(self.db.table(NODBSourceFile.TABLE_NAME)))

    def test_raise_non_cnodc_error_during_save(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        pr = ocproc2.ParentRecord()
        pr.metadata['Hello'] = 'World'
        file = self.temp_dir / 'file.json'
        codec = OCProc2JsonCodec()
        codec.dump(file, [pr])
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.NEW
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        qi = self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        qi.status = QueueStatus.LOCKED
        self.db.insert_object(qi)
        with self.assertLogs('cnodc.worker.decoder', 'ERROR'):
            self.worker_controller.test_queue_worker(
                NODBLoaderBadRecordCreation,
                {
                    'queue_name': 'test_intake',
                    'decoder_class': dynamic_name(OCProc2JsonCodec),
                    'error_directory': str(err_dir),
                    '_test_result': ValueError('oh no'),
                },
                qi
            )
        self.assertIs(qi.status, QueueStatus.COMPLETE)
        qi2 = self.db.table(NODBQueueItem.TABLE_NAME)[-1]
        self.assertEqual(qi2.queue_name, 'decode_failure')
        pl = WorkflowPayload.from_queue_item(qi2)
        self.assertIsInstance(pl, SourceFilePayload)
        self.assertEqual(pl.source_uuid, '12345')
        self.assertEqual(pl.received_date, datetime.date(2015, 1, 2))
        obj = NODBWorkingRecord.find_by_source_info(self.db, '12345', '2015-01-02', 0, 0)
        self.assertIsNone(obj)
        self.assertEqual(1, len(self.db.table(NODBSourceFile.TABLE_NAME)))

    def test_record_already_exists(self):
        err_dir = self.temp_dir / 'errors'
        err_dir.mkdir()
        file = self.temp_dir / 'file.json'
        with open(file, "w") as h:
            h.write('[')
            h.write('{"_metadata": {"foo": "bar2"}},')
            h.write('{"_metadata": {"foo": "bar3"}},')
            h.write('{"_metadata": {"foo": "bar"}}')
            h.write(']')
        sf = NODBSourceFile()
        sf.source_uuid = '12345'
        sf.received_date = datetime.date(2015, 1, 2)
        sf.status = SourceFileStatus.NEW
        sf.file_name = 'file.json'
        sf.source_path = str(file)
        self.db.insert_object(sf)
        sp = SourceFilePayload.from_source_file(sf)
        wr = NODBWorkingRecord()
        wr.source_file_uuid = '12345'
        wr.received_date = datetime.date(2015, 1, 2)
        wr.message_idx = 1
        wr.record_idx = 0
        pr = ParentRecord()
        pr.metadata['foo'] = 'bar4'
        wr.record = pr
        self.db.insert_object(wr)
        self.worker_controller.test_queue_worker(
            NODBDecodeLoadWorker,
            {
                'queue_name': 'test_intake',
                'decoder_class': dynamic_name(OCProc2JsonCodec),
                'error_directory': str(err_dir),
            },
            self.worker_controller.payload_to_queue_item(sp, 'test_intake')
        )
        self.assertEqual(3, self.db.rows(NODBWorkingRecord.TABLE_NAME))
        foo_values = []
        for obj in self.db.table(NODBWorkingRecord.TABLE_NAME):
            foo_values.append(obj.record.metadata.best('foo'))
        self.assertEqual(3, len(foo_values))
        self.assertNotIn('bar3', foo_values)


