import contextlib
import datetime
import decimal
import typing as t

import zrlog

import medsutil.ocproc2 as ocproc2
import medsutil.math as amath
from medsutil.units.structures import UnitError
from nodb.controller import NODBPostgresController, PostgresController
from nodb.interface import NODBInstance
from nodb.observations import NODBWorkingRecord, NODBPlatform
from medsutil.seawater import eos80_pressure
from medsutil.units import UnitConverter
from autoinject import injector
import medsutil.awaretime as awaretime


class QCComplete(Exception):
    pass


class QCSkipTest(Exception):
    pass


class ReferenceRange:

    def __init__(self,
                 minimum: t.Optional[float] = None,
                 maximum: t.Optional[float] = None,
                 units: t.Optional[str] = None,
                 value_kwargs: t.Optional[dict] = None):
        self.minimum = minimum
        self.maximum = maximum
        self.units = units
        self.value_kwargs = value_kwargs or {}

    @staticmethod
    def from_map(map_: dict):
        return ReferenceRange(
            float(map_['minimum']) if 'minimum' in map_ else None,
            float(map_['maximum']) if 'maximum' in map_ else None,
            map_['units'] if 'units' in map_ else None,
            map_['kwargs'] if 'kwargs' in map_ else None
        )

    @staticmethod
    def from_map_of_maps(big_map_: dict) -> dict:
        return {
            k: ReferenceRange.from_map(big_map_[k])
            for k in big_map_
        }


class TestContext:

    def __init__(self,
                 record: ocproc2.ParentRecord,
                 batch_context: dict,
                 working_record: NODBWorkingRecord | None = None):
        self.batch_context: dict = batch_context
        self.qc_messages: list[ocproc2.QCMessage] = []
        self.top_record: ocproc2.ParentRecord = record
        self.current_test_id: str | None = None
        self.current_record: ocproc2.BaseRecord | None = record
        self.current_subrecord_type: t.Optional[str] = None
        self.current_path: list[str] = []
        self.current_recordset: t.Optional[ocproc2.RecordSet] = None
        self.current_value: t.Optional[ocproc2.AbstractElement] = None
        self.other_current_value: t.Optional[ocproc2.AbstractElement] = None
        self.result = ocproc2.QCResult.PASS
        self.working_record: NODBWorkingRecord | None = working_record
        self.test_tags = set()
        self._station: t.Optional[NODBPlatform] = None

    @contextlib.contextmanager
    def test_id_context(self, test_id: str) -> t.Generator[TestContext, None, None]:
        last_test_id = self.current_test_id
        try:
            self.current_test_id = test_id
            yield self
        finally:
            self.current_test_id = last_test_id

    @contextlib.contextmanager
    def self_context(self) -> t.Generator[TestContext, None, None]:
        try:
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)

    @contextlib.contextmanager
    def multivalue_context(self, subvalue_index: int) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'{subvalue_index}')
            self.current_value = self.current_value.value[subvalue_index]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def two_coordinate_context(self, coordinate_1_name: str, coordinate_2_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        other_value = self.other_current_value
        try:
            self.current_path.append(f'coordinates/{coordinate_1_name}')
            self.current_value = self.current_record.coordinates.get(coordinate_1_name)
            self.other_current_value = self.current_record.coordinates.get(coordinate_2_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value
            self.other_current_value = other_value

    @contextlib.contextmanager
    def two_parameter_context(self, parameter_1_name: str, parameter_2_name: str, test_id: str | None = None) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        other_value = self.other_current_value
        try:
            self.current_path.append(f'parameters/{parameter_1_name}')
            self.current_value = self.current_record.parameters.get(parameter_1_name)
            self.other_current_value = self.current_record.parameters.get(parameter_2_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value
            self.other_current_value = other_value

    @contextlib.contextmanager
    def element_context(self, element_name: str) -> t.Generator[TestContext, None, None]:
        if element_name in self.current_record.metadata:
            with self.metadata_context(element_name) as ctx:
                yield ctx
        elif element_name in self.current_record.coordinates:
            with self.coordinate_context(element_name) as ctx:
                yield ctx
        elif element_name in self.current_record.parameters:
            with self.parameter_context(element_name) as ctx:
                yield ctx

    @contextlib.contextmanager
    def parameter_context(self, parameter_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'parameters/{parameter_name}')
            self.current_value = self.current_record.parameters.get(parameter_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def coordinate_context(self, coordinate_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'coordinates/{coordinate_name}')
            self.current_value = self.current_record.coordinates.get(coordinate_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def metadata_context(self, metadata_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            if self.current_value is not None:
                self.current_value = self.current_value.metadata.get(metadata_name)
            else:
                self.current_value = self.current_record.metadata.get(metadata_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def recordset_metadata_context(self, metadata_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            self.current_value = self.current_recordset.metadata.get(metadata_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def element_metadata_context(self, metadata_name: str) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'metadata/{metadata_name}')
            self.current_value = self.current_value.metadata.get(metadata_name)
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_value = last_value

    @contextlib.contextmanager
    def subrecord_context(self, subrecordset_type: str, subrecordset_idx: int, record_idx: int) -> t.Generator[TestContext, None, None]:
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
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_recordset = last_recordset
            self.current_record = last_current

    @contextlib.contextmanager
    def subrecordset_context(self, subrecordset_type: str, subrecordset_idx: int) -> t.Generator[TestContext, None, None]:
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
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
        finally:
            self.current_path.pop()
            self.current_recordset = last_recordset
            self.current_record = last_current

    @contextlib.contextmanager
    def subrecord_from_current_set_context(self, record_idx: int) -> t.Generator[TestContext, None, None]:
        last_current = self.current_record
        try:
            self.current_path.append(f'{record_idx}')
            self.current_record = self.current_recordset.records[record_idx]
            yield self
        except QCSkipTest:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.error_code, ex.ref_value)
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

    def report_for_review(self, flag: int | None, code: str, ref_value=None, subpath: t.Optional[list[str]] = None):
        if flag is not None and self.current_value is not None:
            set_recommended_quality(self.current_value, flag, self.current_test_id)

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

    def __init__(self, test_id: str, test_type: str = 'tests'):
        self.fn = None
        self._test_id = test_id
        self._test_type = test_type

    def set_function(self, fn):
        self.fn = fn

    def __call__(self, fn: t.Callable):
        setattr(fn, 'QC_TEST_WRAPPER', self)
        setattr(fn, 'QC_TEST_TYPE', self._test_type)
        return fn

    def execute_on_context(self, ctx: TestContext):
        with ctx.test_id_context(self._test_id) as ctx:
            self.fn(ctx)


class BatchTest(_TestWrapper):

    def __init__(self, test_id: str):
        super().__init__(test_id, 'batch_tests')

    def __call__(self, fn: t.Callable[[dict[str, TestContext]], t.Any]):
        return super().__call__(fn)

    def execute_on_batch(self, batch: dict[str, TestContext]):
        self.fn(batch)


class RecordSetTest(_TestWrapper):

    def __init__(self, test_id: str, subrecord_type: t.Optional[str] = None):
        super().__init__(test_id, 'subrecord_tests')
        self.subrecord_type = subrecord_type

    def __call__(self, fn: t.Callable[[TestContext], None]):
        return super().__call__(fn)


class RecordTest(_TestWrapper):

    CHILD = 1
    TOP = 2
    ANY = 3

    def __init__(self, test_id: str, subrecord_type: t.Optional[str] = None, record_mode: int = ANY):
        super().__init__(test_id)
        self.subrecord_type = subrecord_type
        self.record_mode = record_mode if subrecord_type is None else self.CHILD

    def __call__(self, fn: t.Callable[[ocproc2.BaseRecord, TestContext], None]):
        return super().__call__(fn)

    def execute_on_context(self, ctx: TestContext):
        if ctx.current_record is None:
            return
        if not should_test_record(ctx.current_record):
            return
        if self.record_mode == RecordTest.TOP and not ctx.is_top_level():
            return
        if self.record_mode == RecordTest.CHILD and ctx.is_top_level():
            return
        if self.subrecord_type is not None and not ctx.current_subrecord_type == self.subrecord_type:
            return
        with ctx.test_id_context(self._test_id) as ctx:
            self.fn(ctx.current_record, ctx)


class _ValueTest(_TestWrapper):

    def __init__(self,
                 test_id: str,
                 value_type: t.Literal['parameters', 'coordinates', 'metadata', 'element_metadata'],
                 value_name: str,
                 skip_empty: bool = True,
                 skip_bad: bool = True,
                 skip_dubious: bool = False,
                 auto_iterate: bool = True,):
        super().__init__(test_id)
        self.value_type = value_type
        self.value_name = value_name
        self.skip_empty = skip_empty
        self.skip_bad = skip_bad
        self.skip_dubious = skip_dubious
        self.auto_iterate = auto_iterate

    def __call__(self, fn: t.Callable[[ocproc2.AbstractElement | ocproc2.SingleElement | ocproc2.MultiElement], None]):
        return super().__call__(fn)

    def execute_on_context(self, ctx: TestContext):
        if ctx.current_record is None:
            return
        if not should_test_record(ctx.current_record):
            return
        with ctx.test_id_context(self._test_id) as ctx:
            self._execute_on_context(ctx)

    def _execute_on_context(self, ctx: TestContext):
        ...

    def execute_on_value(self, value: ocproc2.AbstractElement, ctx: TestContext):
        if isinstance(value, ocproc2.MultiElement):
            if self.auto_iterate:
                self.execute_on_multi_value(value, ctx)
            else:
                self.execute_on_single_value(value, ctx)
        elif isinstance(value, ocproc2.SingleElement):
            self.execute_on_single_value(value, ctx)
        else:
            raise TypeError("unknown value type")

    def execute_on_multi_value(self, value: ocproc2.MultiElement, ctx: TestContext):
        for idx, v in enumerate(value.all_values()):
            if should_test_value(self._test_id, value, self.skip_empty, self.skip_dubious, self.skip_bad):
                with ctx.multivalue_context(idx) as subctx:
                    self.execute_on_single_value(v, subctx)

    def execute_on_single_value(self, value: ocproc2.AbstractElement, ctx: TestContext):
        if should_test_value(self._test_id, value, self.skip_empty, self.skip_dubious, self.skip_dubious):
            self.fn(value, ctx)


def should_test_record(record: ocproc2.BaseRecord) -> bool:
    return True

def should_test_value(test_id: str | None,
                      value: ocproc2.AbstractElement,
                      skip_empty: bool = True,
                      skip_dubious: bool = True,
                      skip_erroneous: bool = False) -> bool:

    try:
        quality = value.metadata.best("Quality", coerce=int, default=0)

        if quality > 0:
            return False

        working_qc = value.metadata.best("WorkingQuality", coerce=int, default=0)
        if skip_empty and (working_qc == 9 or value.is_empty()):
            return False
        elif working_qc == 3 and skip_dubious:
            return False
        elif working_qc == 4 and skip_erroneous:
            return False

        test_id_check = value.metadata.best("SystemIdentifier", coerce=str, default=None)
        if test_id is not None and test_id_check is not None and test_id_check == test_id:
            user_qc = value.metadata.best("UserProvidedQuality", coerce=int, default=None)
            if user_qc is not None:
                if user_qc != 0:
                    set_working_quality(value, user_qc)
                return False
        return True
    finally:
        for key in ("UserProvidedQuality", "SystemIdentifier", "SystemRecommendedQuality"):
            if key in value.metadata:
                del value.metadata[key]


QC_FLAG_PRIORITY = (9, 7, 4, 3, 2, 1, 5)

def set_working_quality(value: ocproc2.AbstractElement, new_working_quality: int):
    existing_qc = value.metadata.best("WorkingQuality", coerce=int, default=0)
    if existing_qc > 0:
        existing_idx = QC_FLAG_PRIORITY.index(existing_qc)
        new_idx = QC_FLAG_PRIORITY.index(new_working_quality)
        if new_idx >= existing_idx:
            return
    value.metadata["WorkingQuality"] = new_working_quality

def set_recommended_quality(value: ocproc2.AbstractElement, recommended_quality: int, test_id: str | None = None):
    value.metadata["SystemRecommendedQuality"] = recommended_quality
    if test_id is not None:
        value.metadata["SystemIdentifier"] = test_id


QC_METADATA_KEYS = ("SystemRecommendedQuality", "SystemIdentifier", "WorkingQuality", "UserProvidedQuality")

class CoordinateTest(_ValueTest):

    def __init__(self, test_id: str, coordinate_name: str, *args, **kwargs):
        super().__init__(test_id, "coordinates", coordinate_name, *args, **kwargs)

    def _execute_on_context(self, ctx: TestContext):
        if self.value_name in ctx.current_record.coordinates:
            with ctx.coordinate_context(self.value_name) as ctx:
                self.execute_on_value(ctx.current_record.coordinates[self.value_name], ctx)


class MetadataTest(_ValueTest):

    def __init__(self, test_id: str, metadata_name: str, *args, **kwargs):
        super().__init__(test_id, "metadata", metadata_name, *args, **kwargs)

    def _execute_on_context(self, ctx: TestContext):
        for p_name, param in ctx.current_record.parameters.items():
            with ctx.parameter_context(p_name) as ctx:
                self._check_element_metadata(param, ctx)
        for m_name, metadata in ctx.current_record.metadata.items():
            with ctx.metadata_context(m_name) as ctx:
                if m_name == self.value_name:
                    self.execute_on_value(metadata, ctx)
                self._check_element_metadata(metadata, ctx)
        for c_name, coordinate in ctx.current_record.coordinates.items():
            with ctx.metadata_context(c_name) as ctx:
                self._check_element_metadata(coordinate, ctx)
        for rs_type in ctx.current_record.subrecords.record_sets:
            for rs_idx, rs in ctx.current_record.subrecords.record_sets[rs_type].items():
                with ctx.subrecordset_context(rs_type, rs_idx) as ctx:
                    self._check_element_metadata(rs, ctx)

    def _check_element_metadata(self, obj: ocproc2.AbstractElement | ocproc2.RecordSet, ctx: TestContext):
        for key in obj.metadata:
            if key in QC_METADATA_KEYS:
                continue
            with ctx.metadata_context(key) as ctx:
                if key == self.value_name:
                    self.execute_on_value(obj.metadata[key], ctx)
                self._check_element_metadata(obj.metadata[key], ctx)


class ParameterTest(_ValueTest):

    def __init__(self, test_id: str, parameter_name: str, *args, **kwargs):
        super().__init__(test_id, "parameters", parameter_name, *args, **kwargs)

    def _execute_on_context(self, ctx: TestContext):
        if self.value_name in ctx.current_record.parameters:
            with ctx.parameter_context(self.value_name) as ctx:
                self.execute_on_value(ctx.current_record.parameters[self.value_name], ctx)

class BaseTestSuite:

    converter: UnitConverter = None
    nodb: NODBPostgresController = None

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
        self._db: t.Optional[NODBInstance] = None
        self._log = zrlog.get_logger(f"qc.test.{qc_test_name}")
        self._tests: dict[str, list[_TestWrapper]] | None = None

    def tests(self) -> dict[str, list[_TestWrapper]]:
        if self._tests is None:
            self._tests = {}
            for attr_name in dir(self):
                attr = getattr(self, attr_name)
                if callable(attr) and hasattr(attr, 'QC_TEST_WRAPPER') and hasattr(attr, 'QC_TEST_TYPE'):
                    t_type = getattr(attr, 'QC_TEST_TYPE')
                    if t_type not in self._tests:
                        self._tests[t_type] = []
                    wrapper: _TestWrapper = getattr(attr, 'QC_TEST_WRAPPER')
                    wrapper.set_function(attr)
                    self._tests[t_type].append(wrapper)
        return t.cast(dict, self._tests)

    def set_db_instance(self, db: NODBInstance):
        self._db = db

    def clear_db_instance(self):
        self._db = None

    def _has_batch_tests(self) -> bool:
        return "batch_tests" in self.tests()

    def _get_tests(self, test_type) -> list[_TestWrapper]:
        tests = self.tests()
        if test_type in tests:
            return self.tests()[test_type]
        return []

    def _get_batch_tests(self) -> list[BatchTest]:
        return t.cast(list[BatchTest], self._get_tests("batch_tests"))

    def _get_recordset_tests(self) -> list[RecordSetTest]:
        return t.cast(list[RecordSetTest], self._get_tests("recordset_tests"))

    def _get_qc_tests(self) -> t.Iterable[_TestWrapper]:
        return self._get_tests("tests")

    def run_batch(self, contexts: dict[str, TestContext]):
        if contexts:
            if self._has_batch_tests():
                for batch_test in self._get_batch_tests():
                    batch_test.execute_on_batch(contexts)
            for context_key in contexts:
                self.run_tests(contexts[context_key])
                self._handle_qc_result(contexts[context_key])

    def run_tests(self, context: TestContext):
        last_result = context.top_record.latest_test_result(self.test_name)
        if last_result:
            return
        with context.self_context():
            self._run_record_tests_and_iterate(context)

    def _handle_qc_result(self, context: TestContext):
        context.top_record.record_qc_test_result(
            test_name=self.test_name,
            test_version=self.test_version,
            test_tags=self.test_tags,
            outcome=context.result,
            messages=context.qc_messages,
        )

    def _run_record_tests_and_iterate(self, context: TestContext):
        self._run_record_tests(context)
        for sr, sr_ctx in self.iterate_on_subrecords(context):
            self._run_record_tests_and_iterate(sr_ctx)

    def _run_record_tests(self, context: TestContext):
        for test in self._get_qc_tests():
            with context.self_context():
                test.execute_on_context(context)
                self._run_recordset_tests(context)

    def _run_recordset_tests(self, context: TestContext):
        if not context.current_record.subrecords:
            return
        tests = self._get_recordset_tests()
        if not tests:
            return
        srts = list(set(x.subrecord_type for x in tests))
        srts.sort()
        for srt in srts:
            if srt is None or srt not in context.current_record.subrecords:
                continue
            for rs_idx in context.current_record.subrecords[srt]:
                for test in tests:
                    if test.subrecord_type == srt:
                        with context.subrecordset_context(srt, rs_idx) as ctx2:
                            test.execute_on_context(ctx2)

    def report_for_review(self,
                          error_code: str,
                          qc_flag: t.Optional[int] = None,
                          ref_value=None):
        raise QCAssertionError(error_code, qc_flag, ref_value)

    def should_test_value(self,
                          value: ocproc2.AbstractElement,
                          ctx: TestContext,
                          /,
                          raise_ex: bool = True,
                          skip_dubious: bool = False,
                          skip_erroneous: bool = True,
                          skip_empty: bool = True) -> bool:
        result = should_test_value(ctx.current_test_id, value, skip_empty, skip_dubious, skip_erroneous)
        if raise_ex and not result:
            raise QCSkipTest()
        return result

    def assert_true(self, v: bool, error_code: str, qc_flag: t.Optional[int] = 4, ref_value=None):
        if not v:
            self.report_for_review(error_code, qc_flag, ref_value)

    def assert_not_empty(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 9):
        if value.is_empty():
            self.report_for_review(error_code, qc_flag)

    def assert_empty(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if not value.is_empty():
            self.report_for_review(error_code, qc_flag)

    def assert_not_multi(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = -1):
        if isinstance(value, ocproc2.MultiElement):
            self.report_for_review(error_code, qc_flag)

    def assert_iso_datetime(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if not value.is_iso_datetime():
            self.report_for_review(error_code, qc_flag)

    def assert_numeric(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if not value.is_numeric():
            self.report_for_review(error_code, qc_flag)

    def assert_integer(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if not value.is_integer():
            self.report_for_review(error_code, qc_flag)

    def assert_string_like(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if isinstance(value.value, (dict, list, tuple, set)):
            self.report_for_review(error_code, qc_flag)

    def assert_list_like(self, value: ocproc2.AbstractElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if not isinstance(value.value, (list, tuple, set)):
            self.report_for_review(error_code, qc_flag)

    def assert_is(self, value, types: type | tuple[type, ...], error_code: str, qc_flag: t.Optional[int] = 4):
        if not isinstance(value, types):
            self.report_for_review(error_code, qc_flag)

    def assert_in(self, value, items: t.Iterable, error_code: str, qc_flag: t.Optional[int] = 4):
        if value not in items:
            self.report_for_review(error_code, qc_flag, ref_value=items)

    def assert_in_past(self, value: ocproc2.SingleElement, error_code: str, qc_flag: t.Optional[int] = 4):
        if value.to_datetime() > awaretime.AwareDateTime.utcnow():
            self.report_for_review(error_code, qc_flag)

    def assert_in_reference_range(self, value: ocproc2.SingleElement, ref: ReferenceRange, error_code: str, qc_flag: t.Optional[int] = 4):
        raw_value = self.value_in_units(value, ref.units, **ref.value_kwargs)
        if raw_value is not None:
            if ref.maximum is not None:
                self.assert_less_than_or_close(raw_value, ref.maximum, error_code=error_code, qc_flag=qc_flag)
            if ref.minimum is not None:
                self.assert_greater_than_or_close(raw_value, ref.minimum, error_code=error_code, qc_flag=qc_flag)

    def assert_compatible_units(self,
                                v: ocproc2.AbstractElement,
                                compatible_units: str,
                                error_code: str,
                                qc_flag: t.Optional[int] = -1,
                                skip_null: bool = True):
        if compatible_units is None:
            return
        un = v.metadata.best('Units', None, coerce=str)
        if un is None or un == '':
            if not skip_null:
                self.report_for_review(error_code, qc_flag, ref_value=compatible_units)
        else:
            try:
                if not self.converter.compatible(compatible_units, un):
                    self.report_for_review(error_code, qc_flag, ref_value=compatible_units)
            except UnitError:
                self.report_for_review(error_code, qc_flag, ref_value=compatible_units)

    def assert_valid_units(self, unit_str: str, error_code: str, qc_flag: t.Optional[int] = -1):
        if not self.converter.is_valid_unit(unit_str):
            self.report_for_review(error_code, qc_flag)

    def assert_has_coordinate(self, record: ocproc2.BaseRecord, coordinate_name: str, error_code: str, qc_flag: t.Optional[int] = 9):
        if coordinate_name not in record.coordinates or record.coordinates[coordinate_name].is_empty():
            self.report_for_review(error_code, qc_flag)

    def assert_between(self, min_val: amath.AnyNumber, value: amath.AnyNumber, max_val: amath.AnyNumber, error_code: str, qc_flag: t.Optional[int] = 4):
        if not (amath.between(min_val, value, max_val) or amath.is_close(min_val, value) or amath.is_close(max_val, value)):
            self.report_for_review(error_code, qc_flag)

    def assert_close_to(self,
                        value: amath.AnyNumber,
                        expected: amath.AnyNumber,
                        error_code: str,
                        qc_flag: t.Optional[int] = 4,
                        rel_tol: str = "1e-09",
                        abs_tol: str = "0.0"):
        if not amath.is_close(value, expected, rel_tol, abs_tol):
            self.report_for_review(error_code, qc_flag)

    def assert_less_than(self,
                         value: amath.AnyNumber,
                         expected: amath.AnyNumber,
                         error_code: str,
                         qc_flag: t.Optional[int] = 4):
        if not amath.lt(value, expected):
            self.report_for_review(error_code, qc_flag, expected)

    def assert_less_than_or_close(self,
                        value: amath.AnyNumber,
                        expected: amath.AnyNumber,
                        error_code: str,
                        qc_flag: t.Optional[int] = 4,
                        rel_tol: str = "1e-09",
                        abs_tol: str = "0.0"):
        if not (amath.lt(value, expected) or amath.is_close(value, expected, rel_tol, abs_tol)):
            self.report_for_review(error_code, qc_flag)

    def assert_greater_than(self,
                            value: amath.AnyNumber,
                            expected: amath.AnyNumber,
                            error_code: str,
                            qc_flag: t.Optional[int] = 4):
        if not amath.gt(value, expected):
            self.report_for_review(error_code, qc_flag, expected)

    def assert_greater_than_or_close(self,
                                     value: amath.AnyNumber,
                                     expected: amath.AnyNumber,
                                     error_code: str,
                                     qc_flag: t.Optional[int] = 4,
                                     rel_tol: str = "1e-09",
                                     abs_tol: str = "0.0"):
        if not (amath.gt(value, expected) or amath.is_close(value, expected, rel_tol, abs_tol)):
            self.report_for_review(error_code, qc_flag)

    def test_all_references_in_record(self,
                                      context: TestContext,
                                      test_id: str,
                                      references: dict[str, ReferenceRange],
                                      error_code: str,
                                      qc_flag: t.Optional[int] = 4):
        for ref_name in references:
            with context.element_context(ref_name) as ctx2:
                self.test_all_subvalues(
                    ctx2,
                    test_id,
                    self.assert_in_reference_range,
                    ref=references[ref_name],
                    error_code=error_code,
                    qc_flag=qc_flag
                )

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

    def iterate_on_subvalues(self, context: TestContext) -> t.Iterable[tuple[ocproc2.SingleElement, TestContext]]:
        if isinstance(context.current_value, ocproc2.MultiElement):
            for idx, subv in enumerate(context.current_value.all_values()):
                with context.multivalue_context(idx) as ctx:
                    yield subv, ctx
        elif context.current_value is not None:
            with context.self_context() as ctx:
                yield t.cast(ocproc2.SingleElement, context.current_value), ctx

    def test_all_subvalues(self, context: TestContext, cb: t.Callable, *args, **kwargs):
        for v, ctx in self.iterate_on_subvalues(context):
            with ctx.self_context() as ctx:
                cb(v, ctx, *args, **kwargs)

    def test_all_records_in_recordset(self, context: TestContext, cb, *args, **kwargs):
        if context.current_recordset is not None:
            for idx, record in enumerate(t.cast(t.Iterable, t.cast(object, context.current_recordset.records))):
                with context.subrecord_from_current_set_context(idx) as ctx2:
                    cb(record, ctx2, *args, **kwargs)

    def test_all_subrecords_without_coordinates(self, context: TestContext, cb, *args, **kwargs):
            for subrecord, ctx in self.iterate_on_subrecords(context):
                if subrecord.coordinates.has_value('Latitude') or subrecord.coordinates.has_value('Longitude'):
                    continue
                with ctx.self_context() as ctx:
                    cb(subrecord, ctx, *args, **kwargs)

    def test_all_subrecords(self, context: TestContext, cb, *args, **kwargs):
            for subrecord, ctx in self.iterate_on_subrecords(context):
                with ctx.self_context() as ctx:
                    cb(subrecord, ctx, *args, **kwargs)

    def iterate_on_subrecord_sets(self, context: TestContext) -> t.Iterable[tuple[ocproc2.RecordSet, TestContext]]:
        if context.current_record is not None:
            for srt in context.current_record.subrecords:
                for srs_idx in context.current_record.subrecords[srt]:
                    with context.subrecordset_context(srt, srs_idx) as ctx:
                        yield context.current_record.subrecords[srt][srs_idx], ctx

    def iterate_on_subrecords(self, context: TestContext) -> t.Iterable[tuple[ocproc2.ChildRecord, TestContext]]:
        if context.current_record is not None:
            for srt in context.current_record.subrecords:
                for srs_idx in context.current_record.subrecords[srt]:
                    for sr_idx, sr in enumerate(t.cast(t.Iterable, t.cast(object, context.current_record.subrecords[srt][srs_idx].records))):
                        with context.subrecord_context(srt, srs_idx, sr_idx) as ctx:
                            yield sr, ctx

    def all_values_in_units(self,
                            value: t.Optional[ocproc2.AbstractElement],
                            *args,
                            **kwargs) -> list[t.Union[amath.AnyNumber, None]]:
        return [
            self.value_in_units(v, *args, **kwargs)
            for v in value.all_values()
        ] if value is not None else []

    def value_in_units(self,
                       value: t.Optional[ocproc2.SingleElement],
                       expected_units: t.Optional[str] = None,
                       temp_scale: str = None) -> t.Optional[amath.AnyNumber]:
        if value is None:
            return None
        if not value.is_numeric():
            return None
        raw_v: decimal.Decimal = value.to_decimal()
        raw_un: str | None = value.metadata.best('Units', None, coerce=str)
        if temp_scale is not None and raw_un is not None:
            ref_scale: str | None = value.metadata.best('TemperatureScale', None, coerce=str)
            if ref_scale is not None and ref_scale != temp_scale:
                from medsutil.seawater import eos80_convert_temperature
                if raw_un != '°C':
                    raw_v = self.converter.convert(raw_v, raw_un, '°C')
                    raw_un = '°C'
                raw_v = eos80_convert_temperature(raw_v, t.cast(str, ref_scale), temp_scale)
        if expected_units is not None:
            if raw_un is not None and raw_un != '' and raw_un != expected_units:
                raw_v = self.converter.convert(raw_v, t.cast(str, raw_un), expected_units)
        return raw_v


class QCTestRunner:

    def __init__(self, qc_tests: list[BaseTestSuite]):
        self._qc_tests: list[BaseTestSuite] = qc_tests

    @property
    def has_batch_tests(self) -> bool:
        return any(x._has_batch_tests() for x in self._qc_tests)

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

    def process_batch(self, batch: t.Iterable[NODBWorkingRecord]) -> t.Iterable[tuple[NODBWorkingRecord, ocproc2.ParentRecord, ocproc2.QCResult, bool]]:
        batch_context = {}
        if self.has_batch_tests:
            working_batch: dict[str, tuple[NODBWorkingRecord, ocproc2.ParentRecord]] = {}
            for wr in batch:
                tuple_set = (wr, t.cast(ocproc2.ParentRecord, wr.record))
                working_batch[wr.working_uuid] = tuple_set
            all_contexts = []
            for test in self._qc_tests:
                test_contexts = {x: TestContext(working_batch[x][1], batch_context, working_batch[x][0]) for x in working_batch}
                test.run_batch(test_contexts)
                all_contexts.append(test_contexts)
            results: dict[str, tuple[ocproc2.QCResult, bool]] = self._process_test_results(all_contexts)
            for working_uuid in results:
                yield working_batch[working_uuid][0], working_batch[working_uuid][1], results[working_uuid][0], results[working_uuid][1]
        else:
            for wr in batch:
                all_contexts = []
                record = wr.record
                for test in self._qc_tests:
                    test_contexts = {wr.working_uuid: TestContext(t.cast(ocproc2.ParentRecord, record), batch_context, wr)}
                    test.run_batch(test_contexts)
                    all_contexts.append(test_contexts)
                results = self._process_test_results(all_contexts)
                yield wr, t.cast(ocproc2.ParentRecord, record), results[wr.working_uuid][0], results[wr.working_uuid][1]

    def _process_test_results(self, context_map: list[dict[str, TestContext]]) -> dict[str, tuple[ocproc2.QCResult, bool]]:
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

    def set_db_instance(self, db: NODBInstance):
        for test in self._qc_tests:
            test.set_db_instance(db)

    def clear_db_instance(self):
        for test in self._qc_tests:
            test.clear_db_instance()
