import datetime
import enum
import math
import sys
import typing as t

import zrlog

import cnodc.ocproc2.structures as ocproc2
from cnodc.nodb import NODBController, NODBControllerInstance
from cnodc.units import UnitConverter
from autoinject import injector
import cnodc.nodb.structures as structures
from uncertainties import ufloat, UFloat


class QCComplete(Exception):
    pass


class QCSkipTest(Exception):
    pass


class TestContext:

    def __init__(self, record: ocproc2.DataRecord):
        self.qc_messages: list[ocproc2.QCMessage] = []
        self.top_record: ocproc2.DataRecord = record
        self.current_record: ocproc2.DataRecord = record
        self.current_subrecord_type: t.Optional[str] = None
        self.current_path: list[str] = []
        self.result = ocproc2.QCResult.PASS
        self._station: t.Optional[structures.NODBStation] = None

    def is_top_level(self) -> bool:
        return not self.current_path

    def skip_qc_test(self, raise_ex: bool = True):
        self.result = ocproc2.QCResult.SKIP
        if raise_ex:
            raise QCComplete

    def report_failure(self, code: str, ref_value=None, subpath: t.Optional[list[str]] = None):
        if subpath is None:
            self.qc_messages.append(ocproc2.QCMessage(code, self.current_path, ref_value))
        else:
            self.qc_messages.append(ocproc2.QCMessage(code, [*self.current_path, *subpath], ref_value))
        self.result = ocproc2.QCResult.FAIL

    def report_for_review(self, code: str, ref_value=None, subpath: t.Optional[list[str]] = None):
        if subpath is None:
            self.qc_messages.append(ocproc2.QCMessage(code, self.current_path, ref_value))
        else:
            self.qc_messages.append(ocproc2.QCMessage(code, [*self.current_path, *subpath], ref_value))
        if self.result == ocproc2.QCResult.PASS:
            self.result = ocproc2.QCResult.MANUAL_REVIEW


class QCAssertionError(Exception):

    def __init__(self, error_code: str, flag_number: int = None, ref_value=None):
        self.error_code = error_code
        self.flag_number = flag_number
        self.ref_value = ref_value


class _TestWrapper:

    def __init__(self):
        self.fn = None
        self._owner = None
        self._name = None

    def __call__(self, *args, **kwargs):
        if self._owner is None:
            self.fn = args[0]
            return self
        else:
            return self._call_self(*args, **kwargs)

    def _call_self(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def execute_on_context(self, obj, ctx: TestContext):
        raise NotImplementedError()

    def __set_name__(self, owner, name):
        if not hasattr(owner, '_tests'):
            setattr(owner, '_tests', [])
        setattr(owner, name, self._call_self)
        owner._tests.append(self)
        self._owner = owner
        self._name = name


class BatchTest(_TestWrapper):

    def execute_batch(self, obj, batch: dict[str, TestContext]):
        try:
            return self._call_self(obj, batch)
        except QCSkipTest:
            pass

    def __set_name__(self, owner, name):
        if not hasattr(owner, '_batch_tests'):
            setattr(owner, '_batch_tests', [])
        owner._tests.append(self)
        setattr(owner, name, self.fn)
        setattr(owner, '_preload_batch', True)


class RecordTest(_TestWrapper):

    def __init__(self, subrecord_type: t.Optional[str] = None, top_only: bool = False):
        super().__init__()
        self.subrecord_type = subrecord_type
        self.top_only = top_only

    def execute_on_context(self, obj, ctx: TestContext):
        if self.top_only and not ctx.is_top_level():
            return
        if self.subrecord_type is not None and not ctx.current_subrecord_type == self.subrecord_type:
            return
        try:
            self._call_self(obj, ctx.current_record, ctx)
        except QCSkipTest:
            pass


class _ValueTest(_TestWrapper):

    def __init__(self, skip_empty: bool = True, skip_bad: bool = True, skip_probably_bad: bool = False):
        super().__init__()
        self.skip_empty = skip_empty
        self.skip_bad = skip_bad
        self.skip_probably_bad = skip_probably_bad

    def execute_on_value(self, obj, value: ocproc2.Value, ctx: TestContext, value_subpath: str):
        current_qc_quality = value.metadata.best_value('WorkingQuality', 0)
        if self.skip_empty and (value.is_empty() or current_qc_quality == 9):
            return
        if self.skip_bad and current_qc_quality == 4:
            return
        if self.skip_probably_bad and current_qc_quality == 3:
            return
        old_path = ctx.current_path
        try:
            ctx.current_path = [*old_path, value_subpath]
            self._call_self(obj, value, ctx)
        except QCAssertionError as ex:
            if ex.flag_number is not None:
                value.metadata['WorkingQuality'] = ex.flag_number
            ctx.report_for_review(ex.error_code, ex.ref_value)
        except QCSkipTest as ex:
            pass
        finally:
            ctx.current_path = old_path


class CoordinateTest(_ValueTest):

    def __init__(self, coordinate_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.coordinate_name = coordinate_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.coordinate_name not in ctx.current_record.coordinates:
            return
        self.execute_on_value(obj, ctx.current_record.coordinates[self.coordinate_name], ctx, f"coordinates/{self.coordinate_name}")


class MetadataTest(_ValueTest):

    def __init__(self, metadata_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata_name = metadata_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.metadata_name not in ctx.current_record.metadata:
            return
        self.execute_on_value(obj, ctx.current_record.metadata[self.metadata_name], ctx, f"metadata/{self.metadata_name}")


class ParameterTest(_ValueTest):

    def __init__(self, parameter_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parameter_name = parameter_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.parameter_name not in ctx.current_record.parameters:
            return
        self.execute_on_value(obj, ctx.current_record.parameters[self.parameter_name], ctx, f"parameters/{self.parameter_name}")


@injector.injectable_global
class StationSearcher:

    def find_by_uuid(self, db: NODBControllerInstance, station_uuid: str) -> t.Optional[structures.NODBStation]:
        return structures.NODBStation.find_by_uuid(db, station_uuid)

    def search_stations(self,
                        db: NODBControllerInstance,
                        in_service_time=None,
                        station_id=None,
                        station_name=None,
                        wmo_id=None,
                        wigos_id=None):
        return structures.NODBStation.search(
            db,
            in_service_time=in_service_time,
            station_id=station_id,
            station_name=station_name,
            wmo_id=wmo_id,
            wigos_id=wigos_id
        )


class BaseTestSuite:

    converter: UnitConverter = None
    searcher: StationSearcher = None
    nodb: NODBController = None

    @injector.construct
    def __init__(self, qc_test_name: str, qc_test_version: str, test_runner_id: str = ''):
        self.test_name = qc_test_name
        self.test_version = qc_test_version
        self.test_runner_id = test_runner_id
        self._db: t.Optional[NODBControllerInstance] = None
        self._log = zrlog.get_logger(f"qc.test.{qc_test_name}")

    def set_db_instance(self, db: NODBControllerInstance):
        self._db = db

    def clear_db_instance(self):
        self._db = None

    def _get_batch_tests(self) -> list[BatchTest]:
        if hasattr(self, '_batch_tests'):
            return self._batch_tests
        return []

    def process_batch(self, batch: t.Iterable[structures.NODBWorkingRecord]) -> t.Iterable[tuple[structures.NODBWorkingRecord, ocproc2.DataRecord, ocproc2.QCResult, bool]]:
        batch_tests = self._get_batch_tests()
        if batch_tests:
            working_batch: dict[str, structures.NODBWorkingRecord] = {}
            for wr in batch:
                if self._skip_record(wr):
                    ctx = TestContext(wr.record)
                    ctx.skip_qc_test(False)
                    yield self._handle_qc_result(ctx), True
                else:
                    working_batch[wr.working_uuid] = wr
            contexts = {
                x: TestContext(working_batch[x].record) for x in working_batch
            }
            for bt in batch_tests:
                bt.execute_batch(self, contexts)
            for uuid_ in working_batch:
                yield self._process_record(working_batch[uuid_], contexts[uuid_])
        else:
            for wr in batch:
                if self._skip_record(wr):
                    yield wr, None, ocproc2.QCResult.SKIP, False
                else:
                    yield self._process_record(wr)

    def _skip_record(self, record: structures.NODBWorkingRecord) -> bool:
        skip_tests: list[str] = record.get_metadata('skip_tests', [])
        if self.test_name in skip_tests:
            skip_tests.remove(self.test_name)
            record.set_metadata('skip_tests', skip_tests)
            return True
        return False

    def _process_record(self, working_record: structures.NODBWorkingRecord, context=None) -> tuple[structures.NODBWorkingRecord, ocproc2.DataRecord, ocproc2.QCResult, bool]:
        dr = working_record.record
        outcome, is_modified = self.verify_record(dr, context)
        return working_record, dr, outcome, is_modified

    def verify_record(self,
                      record: ocproc2.DataRecord,
                      force_rerun: bool = False,
                      context: t.Optional[TestContext] = None) -> tuple[ocproc2.QCResult, bool]:
        """Run the test suite on the record and return the result.

        @param record: The record to verify
        @type record: L{cnodc.ocproc2.DataRecord}
        @param force_rerun: If true, previous test results will not be used
        @type force_rerun: bool
        @param context: Allows us to pass in a context (otherwise one will be built fresh)
        @type L{cnodc.qc.base.TestContext}
        @return: A tuple of the result and a boolean indicating if the record was updated during the test
        @rtype: tuple[L{cnodc.ocproc2.QCResult}, bool]
        """
        # Don't re-test discarded records, just halt
        if record.metadata.best_value('CNODCStatus') == 'DISCARDED':
            return ocproc2.QCResult.SKIP, False
        if not force_rerun:
            last_result = record.latest_test_result(self.test_name)
            if last_result:
                return last_result.result, False

        if context is None:
            context = TestContext(record)
        try:
            self._verify_record_and_iterate(context)
        except QCComplete:
            pass
        return self._handle_qc_result(context), True

    def _handle_qc_result(self, context: TestContext) -> ocproc2.QCResult:
        context.top_record.record_qc_test_result(
            test_name=self.test_name,
            test_version=self.test_version,
            outcome=context.result,
            messages=context.qc_messages,
        )
        return context.result

    def _verify_record_and_iterate(self, context: TestContext):
        self._verify_record(context)
        for sr, sr_ctx in self.iterate_on_subrecords(context.current_record, context):
            self._verify_record_and_iterate(sr_ctx)

    def _verify_record(self, context: TestContext):
        for test in self._get_qc_tests():
            test.execute_on_context(self, context)

    def _get_qc_tests(self) -> t.Iterable[_TestWrapper]:
        if hasattr(self, '_tests'):
            return self._tests
        return []

    def _raise_assertion_error(self, error_code: str, qc_flag: t.Optional[int], ref_value=None):
        raise QCAssertionError(error_code, qc_flag, ref_value)

    def assert_not_empty(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 19):
        if value.is_empty():
            self._raise_assertion_error(error_code, qc_flag)

    def assert_empty(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_empty():
            self._raise_assertion_error(error_code, qc_flag)

    def assert_not_multi(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 20):
        if isinstance(value, ocproc2.MultiValue):
            self._raise_assertion_error(error_code, qc_flag)

    def assert_iso_datetime(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_iso_datetime():
            self._raise_assertion_error(error_code, qc_flag)

    def assert_numeric(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_numeric():
            self._raise_assertion_error(error_code, qc_flag)

    def assert_between(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14, min_val=None, max_val=None):
        if not value.in_range(min_value=min_val, max_value=max_val):
            self._raise_assertion_error(error_code, qc_flag)

    def assert_units_compatible(self, value: ocproc2.Value, preferred_units: str, error_code: str, qc_flag: t.Optional[int] = 21):
        if 'Units' in value.metadata and not self.converter.compatible(value.metadata['Units'].value, preferred_units):
            self._raise_assertion_error(error_code, qc_flag)

    def assert_in_past(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14):
        now = datetime.datetime.now(datetime.timezone.utc)
        dt_value = datetime.datetime.fromisoformat(value.value)
        if dt_value > now:
            self._raise_assertion_error(error_code, qc_flag)

    def copy_original_quality(self, value: ocproc2.Value):
        value.metadata['WorkingQuality'] = value.metadata.best_value('Quality', 1)

    def assert_has_coordinate(self, context: TestContext, coordinate_name: str, error_code: str, qc_flag: t.Optional[int] = 19):
        if coordinate_name not in context.current_record.coordinates or context.current_record.coordinates[coordinate_name].is_empty():
            context.report_for_review(error_code)

    def recommend_discard(self, context: TestContext):
        context.top_record.metadata['CNODCOperatorAction'] = 'RECOMMEND_DISCARD'

    def discard_record(self, context: TestContext):
        context.top_record.metadata['CNODCStatus'] = 'DISCARDED'
        context.skip_and_halt()

    def recommend_duplicate(self, context: TestContext, duplicate_uuid: str, duplicate_date: datetime.date):
        context.top_record.metadata['CNODCOperatorAction'] = 'RECOMMEND_DUPLICATE'
        context.top_record.metadata['CNODCWorkingDuplicateID'] = duplicate_uuid
        context.top_record.metadata['CNODCWorkingDuplicateDate'] = duplicate_date.strftime('%Y-%m-%d')

    def mark_as_duplicate(self, context: TestContext, duplicate_uuid: str, duplicate_date: datetime.date):
        context.top_record.metadata['CNODCDuplicateID'] = duplicate_uuid
        context.top_record.metadata['CNODCDuplicateDate'] = duplicate_date.strftime('%Y-%m-%d')
        context.top_record.metadata['CNODCStatus'] = 'DUPLICATE'

    def recommend_archival(self, context: TestContext):
        context.top_record.metadata['CNODCOperatorAction'] = 'RECOMMEND_ARCHIVAL'

    def archive_record(self, context: TestContext):
        context.top_record.metadata['CNODCStatus'] = 'ARCHIVED'

    def recommend_dubious(self, context: TestContext):
        context.top_record.metadata['CNODCOperatorAction'] = 'RECOMMEND_DUBIOUS'

    def mark_as_dubious(self, context: TestContext):
        context.top_record.metadata['CNODCStatus'] = 'DUBIOUS'

    def require_good_value(self, vmap: ocproc2.ValueMap, name: str, allow_dubious: bool = False):
        if name not in vmap:
            raise QCSkipTest
        val = vmap[name]
        if val.is_empty():
            raise QCSkipTest
        wqc_flag = val.metadata.best_value('WorkingQuality', 0)
        if wqc_flag in (4, 9) or (wqc_flag == 3 and not allow_dubious):
            raise QCSkipTest

    def record_note(self, message: str, context: TestContext, on_top: bool = True):
        if on_top:
            context.top_record.record_note(
                message,
                source_name=self.test_name,
                source_version=self.test_version,
                source_instance=self.test_runner_id
            )
        else:
            context.current_record.record_note(
                message,
                source_name=self.test_name,
                source_version=self.test_version,
                source_instance=self.test_runner_id
            )

    def is_close(self,
                 v: t.Union[ocproc2.Value, UFloat, float],
                 expected: t.Union[float, UFloat],
                 expected_units: t.Optional[str] = None,
                 rel_tol: float = 1e-9,
                 abs_tol: float = 0.0,
                 allow_less_than: bool = False,
                 allow_greater_than: bool = False) -> bool:
        if isinstance(v, ocproc2.Value):
            if v.is_empty():
                return False
            bv = v.to_float_with_uncertainty()
            bv_units = v.metadata.best_value('Units', None)
            if bv_units is not None and expected_units is not None and bv_units != expected_units:
                bv = self.converter.convert(bv, bv_units, expected_units)
        else:
            if v is None:
                return False
            bv = v
        # Check the top or bottom difference between the expected and measured values
        if bv > expected:
            if allow_greater_than:
                return True
            bv_lower = bv.nominal_value - bv.std_dev if isinstance(bv, UFloat) else bv
            expected_upper = expected.nominal_value + expected.std_dev if isinstance(expected, UFloat) else expected
            return bv_lower < expected_upper or math.isclose(bv_lower, expected_upper, rel_tol=rel_tol, abs_tol=abs_tol)
        else:
            if allow_less_than:
                return True
            bv_upper = bv.nominal_value + bv.std_dev if isinstance(bv, UFloat) else bv
            expected_lower = expected.nominal_value - expected.std_dev if isinstance(expected, UFloat) else expected
            return bv_upper > expected_lower or math.isclose(bv_upper, expected_lower, rel_tol=rel_tol, abs_tol=abs_tol)

    def is_greater_than(self,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0) -> bool:
        return self.is_close(v, expected, expected_units, rel_tol, abs_tol, allow_greater_than=True)

    def is_less_than(self,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0) -> bool:
        return self.is_close(v, expected, expected_units, rel_tol, abs_tol, allow_less_than=True)

    def load_station(self, context: TestContext) -> t.Optional[structures.NODBStation]:
        if context._station is None:
            context._station = self._load_station(context.top_record.metadata.best_value('CNODCStation'))
        return context._station

    def _load_station(self, station_uuid: str) -> t.Optional[structures.NODBStation]:
        if station_uuid is not None:
            if self._db is None:
                with self.nodb as db:
                    return self.searcher.find_by_uuid(db, station_uuid)
            else:
                return self.searcher.find_by_uuid(self._db, station_uuid)
        return None

    def iterate_on_subrecords(self, record: ocproc2.DataRecord, context: TestContext):
        base_path = context.current_path
        base_record = context.current_record
        for srt in record.subrecords:
            for srs_idx in record.subrecords[srt]:
                for sr_idx, sr in enumerate(record.subrecords[srt][srs_idx].records):
                    context.current_path = [*base_path, f'{srt}/{srs_idx}/{sr_idx}']
                    context.current_record = sr
                    yield sr, context
        context.current_path = base_path
        context.current_record = base_record