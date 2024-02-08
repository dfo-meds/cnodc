import contextlib
import datetime
import enum
import functools
import itertools
import math
import sys
import typing as t

import zrlog

import cnodc.ocproc2.structures as ocproc2
from cnodc.nodb import NODBController, NODBControllerInstance
from cnodc.ocean_math.seawater import eos80_pressure
from cnodc.units import UnitConverter
from autoinject import injector
import cnodc.nodb.structures as structures
from uncertainties import ufloat, UFloat


class QCComplete(Exception):
    pass


class QCSkipTest(Exception):
    pass


class TestContext:

    def __init__(self,
                 record: ocproc2.DataRecord,
                 batch_context: dict,
                 working_record: structures.NODBWorkingRecord = None):
        self.batch_context: dict = batch_context
        self.qc_messages: list[ocproc2.QCMessage] = []
        self.top_record: ocproc2.DataRecord = record
        self.current_record: ocproc2.DataRecord = record
        self.current_subrecord_type: t.Optional[str] = None
        self.current_path: list[str] = []
        self.current_recordset: t.Optional[ocproc2.RecordSet] = None
        self.current_value: t.Optional[ocproc2.AbstractValue] = None
        self.result = ocproc2.QCResult.PASS
        self.working_record = working_record
        self.test_tags = set()
        self._station: t.Optional[structures.NODBStation] = None

    @contextlib.contextmanager
    def self_context(self):
        try:
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)

    @contextlib.contextmanager
    def multivalue_context(self, subvalue_index: int):
        last_value = self.current_value
        try:
            self.current_path.append(f'{subvalue_index}')
            self.current_value = self.current_value.value[subvalue_index]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def parameter_context(self, parameter_name: str):
        last_value = self.current_value
        try:
            self.current_path.append(f'parameters/{parameter_name}')
            self.current_value = self.current_record.parameters[parameter_name]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def coordinate_context(self, coordinate_name: str):
        last_value = self.current_value
        try:
            self.current_path.append(f'coordinates/{coordinate_name}')
            self.current_value = self.current_record.coordinates[coordinate_name]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def metadata_context(self, metadata_name: str):
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            self.current_value = self.current_record.metadata[metadata_name]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def recordset_metadata_context(self, metadata_name: str):
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            self.current_value = self.current_recordset.metadata[metadata_name]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def element_metadata_context(self, metadata_name: str):
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            self.current_value = self.current_value.metadata[metadata_name]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            if ex.flag_number is not None and self.current_value is not None:
                self.current_value.metadata['WorkingQuality'] = ex.flag_number
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def subrecord_context(self, subrecordset_type: str, subrecordset_idx: int, record_idx: int):
        last_current = self.current_record
        last_recordset = self.current_recordset
        try:
            self.current_path.append(f'subrecords/{subrecordset_type}/{subrecordset_idx}/{record_idx}')
            self.current_recordset = self.current_record.subrecords[subrecordset_type][subrecordset_idx]
            self.current_record = self.current_recordset.records[record_idx]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_recordset = last_recordset
            self.current_record = last_current

    @contextlib.contextmanager
    def subrecordset_context(self, subrecordset_type: str, subrecordset_idx: int):
        last_current = self.current_record
        last_recordset = self.current_recordset
        try:
            self.current_path.append(f'subrecords/{subrecordset_type}/{subrecordset_idx}')
            self.current_recordset = self.current_record.subrecords[subrecordset_type][subrecordset_idx]
            self.current_record = None
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_recordset = last_recordset
            self.current_record = last_current

    @contextlib.contextmanager
    def subrecord_from_current_set_context(self, record_idx: int):
        last_current = self.current_record
        try:
            self.current_path.append(f'{record_idx}')
            self.current_record = self.current_recordset.records[record_idx]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_record = last_current

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


class SubRecordArray:

    def __init__(self,
                 record_indexes: list[int],
                 data: dict[str, list[t.Optional[ocproc2.AbstractValue]]]):
        self.data = data
        self.indexes = record_indexes
        self.length = len(self.indexes)

    def get_data(self, key: str, idx: int):
        if key in self.data and self.length > idx > 0:
            return self.data[key][idx]
        return None

    @contextlib.contextmanager
    def row_context(self, row_idx: int, ctx: TestContext) -> TestContext:
        with ctx.subrecord_from_current_set_context(self.indexes[row_idx]) as ctx:
            yield ctx

    def iterate_rows(self, ctx: TestContext):
        for i in range(0, self.length):
            with self.row_context(i, ctx) as row_ctx:
                yield i, row_ctx

    def test_all_rows(self, ctx: TestContext, cb: callable, *args, **kwargs):
        for idx in range(0, self.length):
            with self.row_context(idx, ctx) as row_ctx:
                cb(self, idx, row_ctx, *args, **kwargs)

    def has_good_value(self, key: str, idx: int, allow_dubious: bool = False):
        if key not in self.data:
            return False
        val = self.data[key][idx]
        if val.is_empty():
            return False
        wq = val.metadata.best_value('WorkingQuality', 0)
        if wq in (4, 9, 14, 19) or ((not allow_dubious) and wq in (3, 13)):
            return False
        if isinstance(val, ocproc2.MultiValue):
            for av in val.values():
                if av.is_empty():
                    continue
                wq = av.metadata.best_value('WorkingQuality', 0)
                if wq in (4, 9, 14, 19) or ((not allow_dubious) and wq in (3, 13)):
                    continue
                return True
            return False
        else:
            return True

    def require_parameter(self, key: str):
        if key not in self.data:
            raise QCSkipTest()

    def require_good_value(self, key: str, idx: int, allow_dubious: bool = False):
        if not self.has_good_value(key, idx, allow_dubious):
            raise QCSkipTest()

    def good_values(self, key: str, idx: int, allow_dubious: bool = False) -> t.Iterable[tuple[t.Optional[int], ocproc2.Value]]:
        if key in self.data:
            use_idx = isinstance(self.data[key][idx], ocproc2.MultiValue)
            for idx, av in enumerate(self.data[key][idx].all_values()):
                if av.is_empty():
                    continue
                wq = av.metadata.best_value('WorkingQuality', 0)
                if wq in (4, 9, 14, 19) or ((not allow_dubious) and wq in (3, 13)):
                    continue
                yield (idx if use_idx else None), av


class QCAssertionError(Exception):

    def __init__(self, error_code: str, flag_number: int = None, ref_value=None):
        self.error_code = error_code
        self.flag_number = flag_number
        self.ref_value = ref_value


class _TestWrapper:

    def __init__(self, target_property: str = '_tests'):
        self._target = target_property
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
        pass

    def __set_name__(self, owner, name):
        if not hasattr(owner, self._target):
            setattr(owner, self._target, [self])
        else:
            getattr(owner, self._target).append(self)
        setattr(owner, name, self._call_self)
        self._owner = owner
        self._name = name


class BatchTest(_TestWrapper):

    def execute_batch(self, obj, batch: dict[str, TestContext]):
        return self._call_self(obj, batch)

    def __set_name__(self, owner, name):
        if not hasattr(owner, '_batch_tests'):
            setattr(owner, '_batch_tests', [])
        owner._tests.append(self)
        setattr(owner, name, self.fn)
        setattr(owner, '_preload_batch', True)


class SubRecordArrayTest(_TestWrapper):

    def __init__(self, subrecord_type: str):
        super().__init__('_sr_array_tests')
        self.subrecord_type = subrecord_type

    def execute_on_subrecord_array(self, obj, sr_array: SubRecordArray, ctx: TestContext):
        return self._call_self(obj, sr_array, ctx)


class ProfileTest(SubRecordArrayTest):

    def __init__(self):
        super().__init__('PROFILE')


class ProfileLevelTest(ProfileTest):

    def __init__(self, required_coordinate: t.Optional[str] = None):
        super().__init__()
        self.required_coordinate = required_coordinate

    def execute_on_subrecord_array(self, obj, sr_array: SubRecordArray, ctx: TestContext):
        if self.required_coordinate is not None and self.required_coordinate not in sr_array.data:
            return
        for i in range(0, sr_array.length):
            with ctx.subrecord_from_current_set_context(i) as ctx2:
                self._call_self(obj, sr_array, i, ctx2)


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
        return self._call_self(obj, ctx.current_record, ctx)


class _ValueTest(_TestWrapper):

    def __init__(self, skip_empty: bool = True, skip_bad: bool = True, skip_dubious: bool = False):
        super().__init__()
        self.skip_empty = skip_empty
        self.skip_bad = skip_bad
        self.skip_dubious = skip_dubious

    def execute_on_value(self, obj, value: ocproc2.Value, ctx: TestContext):
        current_qc_quality = value.metadata.best_value('WorkingQuality', 0)
        if self.skip_empty and (value.is_empty() or current_qc_quality == 9):
            return
        if self.skip_bad and current_qc_quality == 4:
            return
        if self.skip_dubious and current_qc_quality == 3:
            return
        self._call_self(obj, value, ctx)


class CoordinateTest(_ValueTest):

    def __init__(self, coordinate_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.coordinate_name = coordinate_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.coordinate_name not in ctx.current_record.coordinates:
            return
        with ctx.coordinate_context(self.coordinate_name) as ctx:
            self.execute_on_value(obj, ctx.current_record.coordinates[self.coordinate_name], ctx)


class MetadataTest(_ValueTest):

    def __init__(self, metadata_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata_name = metadata_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.metadata_name not in ctx.current_record.metadata:
            return
        with ctx.metadata_context(self.metadata_name) as ctx:
            self.execute_on_value(obj, ctx.current_record.metadata[self.metadata_name], ctx)


class ParameterTest(_ValueTest):

    def __init__(self, parameter_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parameter_name = parameter_name

    def execute_on_context(self, obj, ctx: TestContext):
        if self.parameter_name not in ctx.current_record.parameters:
            return
        with ctx.parameter_context(self.parameter_name) as ctx:
            self.execute_on_value(obj, ctx.current_record.parameters[self.parameter_name], ctx)


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
    def __init__(self,
                 qc_test_name: str,
                 qc_test_version: str,
                 test_runner_id: str = '',
                 test_tags: t.Optional[list[str]] = None,
                 working_sort_by: t.Optional[str] = None,
                 station_invariant: bool = True):
        self.test_name = qc_test_name
        self.station_invariant = station_invariant
        self.working_sort_by = working_sort_by
        self.test_version = qc_test_version
        self.test_runner_id = test_runner_id
        self.test_tags = [x for x in test_tags if x is not None] if test_tags else []
        self._db: t.Optional[NODBControllerInstance] = None
        self._log = zrlog.get_logger(f"qc.test.{qc_test_name}")

    def calculate_pressure_in_dbar(self, pressure_val: t.Optional[ocproc2.Value], depth_val: t.Optional[ocproc2.Value], latitude_val: t.Optional[ocproc2.Value]) -> t.Union[None, float, UFloat]:
        if pressure_val is not None:
            pressure_dbar = self.value_in_units(pressure_val, 'dbar')
            if pressure_dbar is not None:
                return pressure_dbar
        if depth_val is not None and latitude_val is not None:
            depth_m = self.value_in_units(depth_val, 'm')
            latitude = self.value_in_units(latitude_val)
            if depth_m is not None and latitude is not None:
                return eos80_pressure(depth_m, latitude)
        return None

    def set_db_instance(self, db: NODBControllerInstance):
        self._db = db

    def clear_db_instance(self):
        self._db = None

    def has_batch_tests(self) -> bool:
        return bool(self._get_batch_tests())

    def _get_batch_tests(self) -> list[BatchTest]:
        if hasattr(self, '_batch_tests'):
            return self._batch_tests
        return []

    def _get_sra_tests(self) -> list[SubRecordArrayTest]:
        if hasattr(self, '_sr_array_tests'):
            return self._sr_array_tests
        return []

    def run_batch(self, contexts: dict[str, TestContext]):
        skips = [x for x in contexts if self._check_skip_test(contexts[x])]
        run_contexts = contexts if not skips else {x: contexts[x] for x in contexts if x not in skips}
        if run_contexts:
            if self.has_batch_tests():
                for batch_test in self._get_batch_tests():
                    batch_test.execute_batch(self, run_contexts)
            for context_key in run_contexts:
                self.run_tests(run_contexts[context_key])
                self._handle_qc_result(run_contexts[context_key])

    def _check_skip_test(self, context: TestContext) -> bool:
        return False

    def run_tests(self, context: TestContext):
        last_result = context.top_record.latest_test_result(self.test_name)
        if last_result:
            return
        with context.self_context():
            self._verify_record_and_iterate(context)

    def _handle_qc_result(self, context: TestContext):
        context.top_record.record_qc_test_result(
            test_name=self.test_name,
            test_version=self.test_version,
            test_tags=self.test_tags,
            outcome=context.result,
            messages=context.qc_messages,
        )

    def _verify_record_and_iterate(self, context: TestContext):
        self._verify_record(context)
        for sr, sr_ctx in self.iterate_on_subrecords(context.current_record, context):
            self._verify_record_and_iterate(sr_ctx)

    def _verify_record(self, context: TestContext):
        for test in self._get_qc_tests():
            with context.self_context():
                test.execute_on_context(self, context)
        self._run_subrecord_tests(context)

    def _run_subrecord_tests(self, context: TestContext):
        if not context.current_record.subrecords:
            return
        tests = self._get_sra_tests()
        if not tests:
            return
        srts = list(set(x.subrecord_type for x in tests))
        srts.sort()
        for srt in srts:
            if srt not in context.current_record.subrecords:
                continue
            for rs_idx in context.current_record.subrecords[srt]:
                sra = self._build_parameter_array(
                    context.current_record.subrecords[srt][rs_idx]
                )
                for test in tests:
                    if test.subrecord_type == srt:
                        with context.subrecordset_context(srt, rs_idx) as ctx2:
                            test.execute_on_subrecord_array(self, sra, ctx2)

    def _get_qc_tests(self) -> t.Iterable[_TestWrapper]:
        if hasattr(self, '_tests'):
            return self._tests
        return []

    def report_for_review(self, error_code: str, qc_flag: t.Optional[int] = None, ref_value=None):
        raise QCAssertionError(error_code, qc_flag, ref_value)

    def assert_true(self, v: bool, error_code: str, qc_flag: t.Optional[int] = None, ref_value=None):
        if not v:
            self.report_for_review(error_code, qc_flag, ref_value)

    def assert_not_empty(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 19):
        if value.is_empty():
            self.report_for_review(error_code, qc_flag)

    def assert_empty(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_empty():
            self.report_for_review(error_code, qc_flag)

    def assert_not_multi(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 20):
        if isinstance(value, ocproc2.MultiValue):
            self.report_for_review(error_code, qc_flag)

    def assert_iso_datetime(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_iso_datetime():
            self.report_for_review(error_code, qc_flag)

    def assert_numeric(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_numeric():
            self.report_for_review(error_code, qc_flag)

    def assert_integer(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if not value.is_integer():
            self.report_for_review(error_code, qc_flag)

    def assert_in(self, value, items: t.Iterable, error_code: str, qc_flag: t.Optional[int] = 14):
        if value not in items:
            self.report_for_review(error_code, qc_flag, ref_value=items)

    def assert_string_like(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if isinstance(value.value, (dict, list, tuple, set)):
            self.report_for_review(error_code, qc_flag)

    def assert_list_like(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14):
        if not isinstance(value.value, (list, tuple, set)):
            self.report_for_review(error_code, qc_flag)

    def assert_between(self, value: ocproc2.AbstractValue, error_code: str, qc_flag: t.Optional[int] = 14, min_val=None, max_val=None):
        if not value.in_range(min_value=min_val, max_value=max_val):
            self.report_for_review(error_code, qc_flag)

    def assert_units_compatible(self, value: ocproc2.Value, preferred_units: str, error_code: str, qc_flag: t.Optional[int] = 21):
        if 'Units' in value.metadata and not self.converter.compatible(value.metadata['Units'].value, preferred_units):
            self.report_for_review(error_code, qc_flag)

    def assert_in_past(self, value: ocproc2.Value, error_code: str, qc_flag: t.Optional[int] = 14):
        now = datetime.datetime.now(datetime.timezone.utc)
        dt_value = datetime.datetime.fromisoformat(value.value)
        if dt_value > now:
            self.report_for_review(error_code, qc_flag)

    def copy_original_quality(self, value: ocproc2.Value):
        value.metadata['WorkingQuality'] = value.metadata.best_value('Quality', 1)

    def assert_compatible_units(self,
                                v: ocproc2.AbstractValue,
                                compatible_units: str,
                                error_code: str,
                                qc_flag: t.Optional[int] = 21,
                                skip_null: bool = True):
        if compatible_units is None:
            return
        un = v.metadata.best_value('Units', None)
        if un is None or un == '':
            if not skip_null:
                self.report_for_review(error_code, qc_flag, ref_value=compatible_units)
        elif not self.converter.compatible(compatible_units, un):
            self.report_for_review(error_code, qc_flag, ref_value=compatible_units)

    def assert_valid_units(self, unit_str: str, error_code: str, qc_flag: t.Optional[int] = 21):
        if not self.converter.is_valid_unit(unit_str):
            self.report_for_review(error_code, qc_flag)

    def assert_has_coordinate(self, record: ocproc2.DataRecord, coordinate_name: str, error_code: str, qc_flag: t.Optional[int] = 19):
        if coordinate_name not in record.coordinates or record.coordinates[coordinate_name].is_empty():
            self.report_for_review(error_code, qc_flag)

    def recommend_discard(self, context: TestContext):
        context.top_record.metadata['CNODCOperatorAction'] = 'RECOMMEND_DISCARD'

    def discard_record(self, context: TestContext):
        context.top_record.metadata['CNODCStatus'] = 'DISCARDED'
        context.skip_qc_test()

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
            self.skip_test()
        val = vmap[name]
        if val.is_empty():
            self.skip_test()
        wqc_flag = val.metadata.best_value('WorkingQuality', 0)
        if wqc_flag in (4, 9) or (wqc_flag == 3 and not allow_dubious):
            self.skip_test()

    def skip_test(self):
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
                 allow_greater_than: bool = False,
                 **kwargs) -> bool:
        if isinstance(v, ocproc2.Value):
            if v.is_empty():
                return False
            bv = self.value_in_units(v, expected_units, **kwargs)
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
                        abs_tol: float = 0,
                        **kwargs) -> bool:
        return self.is_close(v, expected, expected_units, rel_tol, abs_tol, allow_greater_than=True, **kwargs)

    def is_less_than(self,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0,
                        **kwargs) -> bool:
        return self.is_close(v, expected, expected_units, rel_tol, abs_tol, allow_less_than=True, **kwargs)

    def assert_close_to(self,
                        error_code: str,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0.0,
                        qc_flag: t.Optional[int] = 14,
                        **kwargs):
        if not self.is_close(v, expected, expected_units, rel_tol, abs_tol, **kwargs):
            self.report_for_review(error_code, qc_flag)

    def assert_less_than(self,
                        error_code: str,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0,
                        qc_flag: t.Optional[int] = 14,
                        **kwargs):
        if not self.is_less_than(v, expected, expected_units, rel_tol, abs_tol, **kwargs):
            self.report_for_review(error_code, qc_flag, expected if expected_units is None else f"{expected} {expected_units}")

    def assert_greater_than(self,
                        error_code: str,
                        v: t.Union[ocproc2.Value, UFloat, float],
                        expected: t.Union[float, UFloat],
                        expected_units: t.Optional[str] = None,
                        rel_tol: float = 1e-9,
                        abs_tol: float = 0,
                        qc_flag: t.Optional[int] = 14,
                        **kwargs):
        if not self.is_greater_than(v, expected, expected_units, rel_tol, abs_tol, **kwargs):
            self.report_for_review(error_code, qc_flag, expected if expected_units is None else f"{expected} {expected_units}")

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

    def test_all_subvalues(self, value: ocproc2.AbstractValue, context: TestContext, cb: callable, *args, **kwargs):
        if isinstance(value, ocproc2.MultiValue):
            for idx, subv in enumerate(value.values()):
                with context.multivalue_context(idx) as ctx:
                    cb(value, ctx, *args, **kwargs)
        else:
            with context.self_context() as ctx:
                cb(value, ctx, *args, **kwargs)

    def iterate_on_subvalues(self, value: ocproc2.AbstractValue, context: TestContext) -> t.Iterable[ocproc2.Value, TestContext]:
        if isinstance(value, ocproc2.MultiValue):
            for idx, subv in enumerate(value.values()):
                with context.multivalue_context(idx) as ctx:
                    yield subv, ctx
        else:
            with context.self_context() as ctx:
                yield value, ctx

    def iterate_on_subrecord_sets(self, record: ocproc2.DataRecord, context: TestContext) -> t.Iterable[tuple[ocproc2.RecordSet, TestContext]]:
        for srt in record.subrecords:
            for srs_idx in record.subrecords[srt]:
                with context.subrecordset_context(srt, srs_idx) as ctx:
                    yield record.subrecords[srt][srs_idx], ctx

    def test_all_subrecords(self, record: ocproc2.DataRecord, context: TestContext, cb, *args, **kwargs):
        for srt in record.subrecords:
            for srs_idx in record.subrecords[srt]:
                for sr_idx, sr in enumerate(record.subrecords[srt][srs_idx].records):
                    with context.subrecord_context(srt, srs_idx, sr_idx) as ctx:
                        cb(record, ctx, *args, **kwargs)

    def iterate_on_subrecords(self, record: ocproc2.DataRecord, context: TestContext) -> t.Iterable[tuple[ocproc2.DataRecord, TestContext]]:
        for srt in record.subrecords:
            for srs_idx in record.subrecords[srt]:
                for sr_idx, sr in enumerate(record.subrecords[srt][srs_idx].records):
                    with context.subrecord_context(srt, srs_idx, sr_idx) as ctx:
                        yield sr, ctx

    def value_in_units(self,
                       value: ocproc2.AbstractValue,
                       expected_units: t.Optional[str] = None,
                       temp_scale: str = None,
                       null_dubious: bool = False,
                       null_erroneous: bool = False):
        for v in value.all_values():
            if v.is_empty():
                continue
            wq = v.metadata.best_value('WorkingQuality', 0)
            if wq in (9, 19):
                continue
            if wq in (3, 13) and null_dubious:
                continue
            if wq in (4, 14) and null_erroneous:
                continue
            raw_v = v.to_float_with_uncertainty()
            raw_un = v.metadata.best_value('Units', None)
            if temp_scale is not None:
                ref_scale = v.metadata.best_value('TemperatureScale', None)
                if ref_scale is not None and ref_scale != temp_scale:
                    from cnodc.ocean_math.seawater import eos80_convert_temperature
                    if raw_un != '°C':
                        raw_v = self.converter.convert(raw_v, raw_un, '°C')
                        raw_un = '°C'
                    raw_v = eos80_convert_temperature(raw_v, ref_scale, temp_scale)
            if expected_units is not None:
                if raw_un is not None and raw_un != '' and raw_un != expected_units:
                    raw_v = self.converter.convert(raw_v, raw_un, expected_units)
            return raw_v
        return None

    def _build_parameter_array(self, recordset: ocproc2.RecordSet) -> SubRecordArray:
        pkeys = set()
        ckeys = set()
        for r in recordset.records:
            pkeys.update(k for k in r.parameters if r.parameters[k].is_numeric())
            ckeys.update(k for k in r.coordinates if r.coordinates[k].is_numeric())
        results = {
            pkey: [] for pkey in itertools.chain(pkeys, ckeys)
        }
        indexes = []
        for idx, r in enumerate(recordset.records):
            indexes.append(idx)
            for pkey in pkeys:
                if pkey in r.parameters:
                    results[pkey].append(r.parameters[pkey])
                else:
                    results[pkey].append(None)
            for ckey in ckeys:
                if ckey in r.coordinates:
                    results[ckey].append(r.coordinates[ckey])
                else:
                    results[ckey].append(None)
        return SubRecordArray(indexes, results)


class QCTestRunner:

    def __init__(self, qc_tests: list[BaseTestSuite]):
        self._qc_tests = qc_tests
        self._has_batch_tests = any(x.has_batch_tests() for x in self._qc_tests)

    @property
    def station_invariant(self):
        return all(x.station_invariant for x in self._qc_tests)

    @property
    def working_sort_by(self):
        sort_order = None
        for t in self._qc_tests:
            if t.working_sort_by is None:
                continue
            if sort_order is not None and sort_order != t.working_sort_by:
                raise ValueError('cannot mix tests with different sort orders')
            else:
                sort_order = t.working_sort_by
        return sort_order

    def test_names(self):
        return [t.test_name for t in self._qc_tests]

    def process_batch(self, batch: t.Iterable[structures.NODBWorkingRecord]) -> t.Iterable[tuple[structures.NODBWorkingRecord, ocproc2.DataRecord, ocproc2.QCResult, bool]]:
        batch_context = {}
        if self._has_batch_tests:
            working_batch: dict[str, tuple[structures.NODBWorkingRecord, ocproc2.DataRecord]] = {}
            for wr in batch:
                record = wr.record
                skip_result = self._check_skip_all(wr, record)
                if skip_result is None:
                    working_batch[wr.working_uuid] = (wr, record)
                else:
                    yield wr, record, *skip_result
            all_contexts = []
            for test in self._qc_tests:
                test_contexts = {x: TestContext(working_batch[x][1], batch_context, working_batch[x][0]) for x in working_batch}
                test.run_batch(test_contexts)
                all_contexts.append(test_contexts)
            results = self._process_test_results(all_contexts)
            for working_uuid in results:
                yield *working_batch[working_uuid], *results[working_uuid]
        else:
            for wr in batch:
                all_contexts = []
                record = wr.record
                skip_result = self._check_skip_all(wr, record)
                if skip_result is None:
                    for test in self._qc_tests:
                        test_contexts = {wr.working_uuid: TestContext(record, batch_context, wr)}
                        test.run_batch(test_contexts)
                        all_contexts.append(test_contexts)
                    results = self._process_test_results(all_contexts)
                    yield wr, record, *results[wr.working_uuid]
                else:
                    yield wr, record, *skip_result

    def _process_test_results(self, context_map: list[dict[str, TestContext]]) -> dict[str, list]:
        results = {}
        for test_outcome in context_map:
            for record_uuid in test_outcome:
                if record_uuid not in results:
                    results[record_uuid] = [ocproc2.QCResult.PASS, False]
                context = test_outcome[record_uuid]
                if context.result == ocproc2.QCResult.MANUAL_REVIEW:
                    results[record_uuid][0] = ocproc2.QCResult.MANUAL_REVIEW
                    results[record_uuid][1] = True
                elif context.result != ocproc2.QCResult.SKIP:
                    results[record_uuid][1] = True
        return results

    def _check_skip_all(self, wr: structures.NODBWorkingRecord, record: ocproc2.DataRecord) -> t.Optional[tuple[ocproc2.QCResult, bool]]:
        return None

    def set_db_instance(self, db: NODBControllerInstance):
        for test in self._qc_tests:
            test.set_db_instance(db)

    def clear_db_instance(self, db: NODBControllerInstance):
        for test in self._qc_tests:
            test.clear_db_instance()
