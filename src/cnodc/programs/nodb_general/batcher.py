import hashlib
import uuid

from cnodc.ocproc2 import DataRecord
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBControllerInstance
from cnodc.programs.nodb_general.qc import NODBVerificationTestSuite
from cnodc.qc import VerificationTestResult
from cnodc.util import HaltFlag
from cnodc.workflow.workflow import WorkflowPayload, BatchPayload


class NODBInitialVerificationBatchManager:

    def __init__(self,
                 original_payload: WorkflowPayload,
                 db: NODBControllerInstance,
                 record_cap: int = 3000000,
                 flush_halt: int = 2000000,
                 for_batch_id: t.Optional[str] = None,
                 fail_queue: str = 'nodb_review',
                 pass_queue: str = 'nodb_integrity',
                 station_creation_queue: str = 'nodb_station_check',
                 halt_flag: HaltFlag = None):
        self._for_batch_id = for_batch_id
        self._fail_queue = fail_queue
        self._station_creation_queue = station_creation_queue
        self._pass_queue = pass_queue
        self._original_payload = original_payload
        self._batch_groups: dict[str, list] = {}
        self._match_failure_info: dict[str, dict] = {}
        self._record_count = 0
        self._record_cap = record_cap
        self._wait_to_flush_no_id = 1000
        self._flush_halt = flush_halt
        self._halt_flag = halt_flag
        self._db = db
        self._test_suite = NODBVerificationTestSuite()

    def verify_record(self, working_record: structures.NODBWorkingRecord):
        if self._for_batch_id is None:
            if working_record.qc_batch_id is not None:
                return
        elif working_record.qc_batch_id != self._for_batch_id:
            return
        record = working_record.record
        test_result = self._test_suite.run_verification(record, self._db)
        if test_result in (VerificationTestResult.PASS, VerificationTestResult.FAIL):
            working_record.record = record
            self._db.update_object(working_record)
            self._db.commit()
        self._record_verification_result(
            working_record.working_uuid,
            record,
            test_result in (VerificationTestResult.PASS, VerificationTestResult.STALE_PASS)
        )

    def _record_verification_result(self, working_uuid: str, record: DataRecord, verification_result: bool):
        group_name = self._get_group_name(record)
        if group_name not in self._batch_groups:
            self._batch_groups[group_name] = [verification_result, [working_uuid]]
        else:
            self._batch_groups[group_name][1].append(working_uuid)
            if not verification_result:
                self._batch_groups[group_name][0] = False
        self._record_count += 1
        self._flush_check()

    def _flush_check(self):
        if self._record_count > self._record_cap:
            for gn in self._flush_priorities():
                self._flush_group(gn)
                if self._record_count < self._flush_halt:
                    break

    def _flush_priorities(self) -> t.Iterable[str]:
        good_batches = []
        bad_batches = []
        for gn in self._batch_groups:
            # Flush no station IDs first
            if gn == '__no_station_id':
                yield gn
            group_ln = len(self._batch_groups[gn][1])
            # Don't flush these if they only have a single entry
            if group_ln == 1 and gn.startswith('__station_group_'):
                continue
            if self._batch_groups[gn][1]:
                good_batches.append((group_ln, gn))
            else:
                bad_batches.append((group_ln, gn))
        good_batches.sort(key=lambda x: x[0], reverse=True)
        bad_batches.sort(key=lambda x: x[0], reverse=True)
        # We have a list sorted by size of good and bad stations
        l_good = len(good_batches)
        l_bad = len(bad_batches)
        good_idx = 0
        bad_idx = 0
        while good_idx < l_good or bad_idx < l_bad:
            # Only bad batches left
            if good_idx >= l_good:
                yield bad_batches[bad_idx][1]
                bad_idx += 1
            # Only good batches left
            elif bad_idx >= l_bad:
                yield good_batches[good_idx][1]
                good_idx += 1
            # Both good and batch batches left, dump the good first
            else:
                yield good_batches[good_idx][1]
                good_idx += 1
                # TODO: do we want to consider a heuristic to dump some very big
                # bad batches before very small good batches?

    def flush_all(self):
        for gn in self._batch_groups.keys():
            self._flush_group(gn)

    def _flush_group(self, group_name: str):
        if self._submit_batch(group_name):
            self._record_count -= len(self._batch_groups[group_name][1])
            del self._batch_groups[group_name]
            if group_name in self._match_failure_info:
                del self._match_failure_info[group_name]

    def _submit_batch(self, group_name: str) -> bool:
        qc_batch = structures.NODBBatch()
        qc_batch.batch_uuid = str(uuid.uuid4())
        target_queue = self._pass_queue
        target_subqueue = None
        if group_name.startswith('__station_group_') and len(self._batch_groups[group_name][1]) > 1:
            target_queue = self._station_creation_queue
            qc_batch.status = structures.BatchStatus.QUEUED
        elif not self._batch_groups[group_name][0]:
            target_queue = self._fail_queue
            target_subqueue = self._original_payload.headers['manual-subqueue'] if 'manual-subqueue' in self._original_payload.headers else None
            qc_batch.status = structures.BatchStatus.MANUAL_REVIEW
        else:
            qc_batch.status = structures.BatchStatus.QUEUED
        self._db.insert_object(qc_batch)
        structures.NODBWorkingRecord.bulk_set_batch_uuid(
            self._db,
            self._batch_groups[group_name][1],
            qc_batch.batch_uuid
        )
        payload = BatchPayload(qc_batch.batch_uuid)
        self._original_payload.update_for_propagation(payload)
        self._db.create_queue_item(
            target_queue,
            payload.to_map(),
            unique_item_key=group_name,
            subqueue_name=target_subqueue
        )
        self._db.commit()
        return True

    def _get_group_name(self, record: DataRecord) -> str:
        # Single station match
        if record.metadata.has_value('CNODCStation'):
            return self._values_to_md5([record.metadata['CNODCStation'].best_value()])
        # Multiple station match
        elif record.metadata.has_value('CNODCStationCandidates'):
            return self._values_to_md5(record.metadata.best_value('CNODCStationCandidates'))
        else:
            values = {
                x: record.metadata.best_value(x)
                for x in ('WMOID', 'StationName', 'WIGOSID', 'StationID', 'CNODCStation')
                if record.metadata.has_value(x)
            }
            # Has many values
            if values:
                md5_hash = self._values_to_md5(f"{x}={values[x]}" for x in values)
                key = f'__station_group_{md5_hash}'
                self._match_failure_info[key] = values
                return key
            # Has no values
            else:
                return '__no_station_id'

    def _values_to_md5(self, v: t.Iterable[str]):
        return hashlib.md5('\x1F'.join(v).encode('utf-8', errors='replace')).hexdigest()
