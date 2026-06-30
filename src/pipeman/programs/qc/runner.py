import hashlib
import typing as t

from medsutil import ocproc2 as ocproc2
from medsutil.exceptions import CodedError
from nodb.interface import NODBInstance, LockType
from nodb.observations import NODBSourceFile, NODBBatch, BatchStatus, NODBWorkingRecord
from pipeman.programs.qc.base import QualityController


class QCTestRunnerError(CodedError): CODE_SPACE = "QC-TEST-RUNNER"


class QCTestRunner:

    def __init__(self,
                 db: NODBInstance,
                 process_id: str,
                 batch_queuer: t.Callable[[NODBInstance, str, int], None],
                 test_definitions: list[tuple[type[QualityController], tuple | list | None, dict[str, t.Any] | None]]):
        self._test_definitions = test_definitions
        self._process_id = process_id
        self._db = db
        self._batch_queuer = batch_queuer

    def qc_source_file(self, sf: NODBSourceFile):
        self._process_working_records(sf.stream_working_records)
        self._flush_results()
        self._db.commit()

    def qc_batch(self, batch: NODBBatch):
        batch.status = BatchStatus.IN_PROGRESS
        self._db.update_object(batch)
        self._db.commit()
        self._process_working_records(batch.stream_working_records)
        self._flush_results(batch.batch_uuid)
        batch.status = BatchStatus.COMPLETE
        self._db.update_object(batch)
        self._db.commit()

    def _process_working_records(self, working_records_streamer: t.Callable[..., t.Iterable[NODBWorkingRecord]]):
        tests, sort_order, batcher = self._build_tests()
        for working_record in working_records_streamer(self._db, order_by=sort_order):
            if not self._db.has_temp_qc_outcome(self._process_id, working_record.working_uuid):
                record = working_record.record
                if record is not None:
                    qc_results = []
                    qc_flags = working_record.qc_flags
                    for test in tests:
                        result, qc_flags = test.run_record_check(
                            record,
                            self._db,
                            qc_flags,
                            working_record.source_file_uuid,
                            working_record.received_date
                        )
                        qc_results.append(result.result)
                    working_record.record = record
                    working_record.qc_flags = qc_flags
                    batch_key, outcome = batcher.assign_batch(working_record, record, qc_results)
                    self._db.update_object(working_record)
                    self._db.create_temp_qc_outcome(self._process_id, t.cast(str, working_record.working_uuid), batch_key, outcome)
                    self._db.commit()

    def _flush_results(self, current_batch_uuid: str | None = None):
        for batch_identifier, outcome in self._db.stream_temp_qc_outcomes(self._process_id):
            new_batch = NODBBatch(status=BatchStatus.NEW)
            self._db.insert_object(new_batch)
            self._db.commit()
            new_batch.status = BatchStatus.QUEUED
            self._db.update_object(new_batch)
            self._db.reassign_temp_qc_outcomes(self._process_id, batch_identifier, outcome, new_batch.batch_uuid, current_batch_uuid)
            self._batch_queuer(self._db, new_batch.batch_uuid, outcome)
            self._db.commit()
        self._db.cleanup_temp_qc_outcomes(self._process_id)

    def _build_tests(self) -> tuple[list[QualityController], str | tuple[str, bool] | None, ResultBatcher]:
        tests: list[QualityController] = []
        sort_order = None
        station_invariant = True
        for test_cls, test_args, test_kwargs in self._test_definitions:
            test = test_cls(*(test_args or []), **(test_kwargs or {}))
            if test.working_sort is not None:
                if sort_order is not None and sort_order != test.working_sort:
                    raise QCTestRunnerError(f"Incompatible sort orders [{test.working_sort}] and [{sort_order}]")
                sort_order = test.working_sort
                if not test.station_invariant:
                    station_invariant = False
            tests.append(test)
        return tests, sort_order, SimpleBatcher() if station_invariant else PlatformBatcher()


class ResultBatcher:

    RESULT_NEXT = 1
    RESULT_REVIEW = 2
    RESULT_ERROR = 3

    def assign_batch(self,
                     working_record: NODBWorkingRecord,
                     record: ocproc2.ParentRecord,
                     qc_results: list[ocproc2.QCResult]) -> tuple[str, int]:
        return self._assign_batch_group(working_record, record), self._assign_batch_result(qc_results)

    def _assign_batch_group(self, working_record: NODBWorkingRecord, record: ocproc2.ParentRecord) -> str:
        raise NotImplementedError

    def _assign_batch_result(self, qc_results: list[ocproc2.QCResult]) -> int:
        result = self.RESULT_NEXT
        for qcr in qc_results:
            if qcr is ocproc2.QCResult.ERROR:
                return self.RESULT_ERROR
            elif qcr is ocproc2.QCResult.MANUAL_REVIEW:
                result = self.RESULT_REVIEW
        return result


class PlatformBatcher(ResultBatcher):
    """ Divides up the results by station """

    def _assign_batch_group(self, working_record: NODBWorkingRecord, record: ocproc2.ParentRecord) -> str:
        platform_info = self._find_platform_info(record)
        if platform_info:
            # Max length for storing platform info in the database
            if len(platform_info) > 1024:
                return hashlib.sha512(platform_info.encode('utf-8'), usedforsecurity=False).hexdigest()
            return platform_info
        return ""

    def _find_platform_info(self, record: ocproc2.ParentRecord) -> str | None:
        if record.metadata.has_value('CNODCPlatform'):
            return record.metadata['CNODCPlatform'].to_string()
        elif record.metadata.has_value('CNODCPlatformCandidates'):
            return '\x1F'.join(str(x) for x in record.metadata['CNODCPlatformCandidates'].value)
        else:
            platform_keys = {
                key: record.metadata[key].to_string()
                for key in ("PlatformID", "PlatformName", "WMOID", "WIGOSID")
                if record.metadata.has_value(key)
            }
            if platform_keys:
                return "\x1f".join(f"{k}: {v}" for k, v in platform_keys.items())
        return None


class SimpleBatcher(ResultBatcher):
    """ Keeps all the results together, except divided by outcome. """

    def _assign_batch_group(self, working_record: NODBWorkingRecord, record: ocproc2.ParentRecord) -> str:
        return working_record.qc_batch_id or f"{working_record.source_file_uuid}__{working_record.received_date}"
