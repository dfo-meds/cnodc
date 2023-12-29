import enum
import typing as t
from cnodc.ocproc2 import DataRecord
from cnodc.ocproc2.structures import QCMessage


class VerificationTestResult(enum.Enum):

    PASS = 'PASS'
    FAIL = 'FAIL'
    STALE_PASS = 'STALE_PASS'
    STALE_FAIL = 'STALE_FAIL'


class TestContext:

    def __init__(self, record: DataRecord):
        self.qc_messages: list[QCMessage] = []
        self.top_record: DataRecord = record
        self.current_record: DataRecord = record
        self.current_subrecord_type: t.Optional[str] = None
        self.current_path: list[str] = []

    def is_top_level(self) -> bool:
        return not self.current_path

    def report_qc_failure(self, code: str, ref_value=None):
        self.qc_messages.append(QCMessage(code, self.current_path, ref_value))


class CNODCBaseQCTestSuite:

    def __init__(self, qc_test_name: str, qc_test_version: str):
        self._qc_test_name = qc_test_name
        self._qc_test_version = qc_test_version

    def verify_record(self, record: DataRecord, force_rerun: bool = False) -> VerificationTestResult:
        if not force_rerun:
            last_result = record.latest_test_result(self._qc_test_name)
            if last_result is not None:
                return VerificationTestResult.STALE_PASS if last_result.passed() else VerificationTestResult.STALE_FAIL
        context = TestContext(record)
        self._verify_record_and_iterate(context)
        self._handle_qc_result(context)

    def _handle_qc_result(self, context: TestContext) -> bool:
        if context.qc_messages:
            context.top_record.record_qc_test_failed(
                test_name=self._qc_test_name,
                test_version=self._qc_test_version,
                messages=context.qc_messages
            )
            return False
        else:
            context.top_record.record_qc_test_passed(
                test_name=self._qc_test_name,
                test_version=self._qc_test_version
            )
            return True

    def _verify_record_and_iterate(self, context: TestContext):
        self._verify_record(context)
        current_path = context.current_path
        for subrecord_type in context.current_record.subrecords:
            context.current_subrecord_type = subrecord_type
            for subrecord_set_idx in context.current_record.subrecords[subrecord_type]:
                for subrecord_idx, sub_record in enumerate(context.current_record.subrecords[subrecord_type][subrecord_set_idx].records):
                    context.current_path = [*current_path, f"{subrecord_type}/{subrecord_set_idx}/{subrecord_idx}"]
                    context.current_record = sub_record
                    self._verify_record_and_iterate(context)

    def _verify_record(self, context: TestContext):
        pass
