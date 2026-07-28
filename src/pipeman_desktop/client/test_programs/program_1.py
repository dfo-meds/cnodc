import datetime
import typing as t
import uuid

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, SingleElement, QCTestRunInfo, QCResult, QCMessage, ChangeQuality, \
    ChildRecord, RecordSet
from medsutil.ocproc2.operations import SetManualQCOutcome
from nodb.observations import NODBWorkingRecord

if t.TYPE_CHECKING:
    from pipeman_desktop.client.test_client import MockNODB

def setup(nodb: MockNODB):
    nodb.add_queue_item(
        _build_12345_records(),
        "12345",
        "gtspp_qca",
    )
    nodb.add_batch_qc_endpoint("gtspp_qca", 0, None)

def _build_12345_records() -> t.Iterable[tuple[NODBWorkingRecord, list[dict] | None]]:
    start_time = AwareDateTime(2015, 1, 2, 3, 4, tzinfo="Etc/UTC")
    start_lat = -45.125
    start_lon = -123.515

    for x in range(0, 10):
        record = ParentRecord()
        record.coordinates["Time"] = SingleElement(
            start_time + datetime.timedelta(minutes=15 * x),
            DatePrecision="minute"
        )
        record.coordinates["Latitude"] = SingleElement(
            start_lat + (0.0009 * x),
            Units="degrees_north"
        )
        record.coordinates["Longitude"] = SingleElement(
            start_lon - (0.0004 * x),
            Units = "degrees_north"
        )
        record.parameters["Temperature"] = SingleElement(
            8312.31,
            Units="K"
        )
        rs = RecordSet()
        for y in range(0, 20):
            record = ChildRecord()
            record.coordinates["Depth"] = SingleElement(25 + (y * 50), Units="m")
            record.coordinates["Temperature"] = SingleElement(287.12 + (0.01 * x) + (0.004 * y), Units = "K")
            rs.records.append(record)
        record.subrecords.record_sets["PROFILE"] = {0: rs}
        qc_test = QCTestRunInfo(
            "fake", "1.0", AwareDateTime.utcnow(), QCResult.MANUAL_REVIEW, [
                QCMessage("failed_temperature_check", "parameters/Temperature", review_name="oh_no")
            ], None, False, None, [
                ChangeQuality(path="parameters/Temperature", new_flag=4),
                SetManualQCOutcome(qc_index=0, actual_result=QCResult.MANUAL_FAIL)
            ], None
        )
        record.qc_tests.append(qc_test)
        w_record = NODBWorkingRecord()
        w_record.working_uuid = str(uuid.uuid4())
        w_record.record = record
        w_record.source_file_uuid = "23456"
        w_record.message_idx = x
        w_record.record_idx = 0
        w_record.platform_uuid = "34567"
        yield w_record, None
