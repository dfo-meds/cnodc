import abc
import collections.abc
import contextlib
import dataclasses
import datetime
import decimal
import enum
import functools
import hashlib
import typing as t

import zrlog

import medsutil.ocproc2 as ocproc2
import medsutil.math as amath
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.ocproc2 import QCInfo
from medsutil.units.structures import UnitError
from nodb.controller import NODBPostgresController, PostgresController
from nodb.interface import NODBInstance, LockType
from nodb.observations import NODBWorkingRecord, NODBPlatform, NODBBatch, BatchStatus, NODBSourceFile
from medsutil.seawater import eos80_pressure
from medsutil.units import UnitConverter
from autoinject import injector
import medsutil.awaretime as awaretime

if t.TYPE_CHECKING:
    class QCMethodProtocol[R: AnyRef](t.Protocol):
        def __call__(_, self: QualityChecker, ref: R, *args, **kwargs) -> t.Any:
            ...


class QCException(Exception): ...

class QCComplete(QCException): ...

class QCSkipReview(QCException): ...

class QCSkipTest(QCException): ...

class QCAssertionError(QCException):

    def __init__(self,
                 error_code: str,
                 flag_number: int = None,
                 ref_value: t.Any = None,
                 subpath: t.Optional[str] = None):
        self.error_code = error_code
        self.flag_number = flag_number
        self.ref_value = ref_value
        self.subpath = subpath


"""

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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)

    @contextlib.contextmanager
    def multivalue_context(self, subvalue_index: int) -> t.Generator[TestContext, None, None]:
        last_value = self.current_value
        try:
            self.current_path.append(f'{subvalue_index}')
            self.current_value = self.current_value.value[subvalue_index]
            yield self
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
        except QCSkipCheck:
            pass
        except QCAssertionError as ex:
            self.report_for_review(ex.flag_number, ex.specific_test_name, ex.ref_value)
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
            raise QCSkipCheck()
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
        raise QCSkipCheck

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

"""


class QCTestRunnerError(CodedError): CODE_SPACE = "QC-TEST-RUNNER"

class QCTestRunner:

    def __init__(self,
                 db: NODBInstance,
                 process_id: str,
                 batch_queuer: t.Callable[[NODBInstance, str, int], None],
                 test_definitions: list[tuple[type[QualityChecker], tuple | list | None, dict[str, t.Any] | None]]):
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
                    for test in tests:
                        result = test.run_record_check(record)
                        qc_results.append(result.result)
                    working_record.record = record
                    batch_key, outcome = batcher.assign_batch(working_record, record, qc_results)
                    self._db.update_object(working_record)
                    self._db.create_temp_qc_outcome(self._process_id, working_record.working_uuid, batch_key, outcome)
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

    def _build_tests(self) -> tuple[list[QualityChecker], str | tuple[str, bool] | None, ResultBatcher]:
        tests: list[QualityChecker] = []
        sort_order = None
        station_invariant = True
        for test_cls, test_args, test_kwargs in self._test_definitions:
            test = test_cls(*(test_args or []), **(test_kwargs or {}))
            if test.working_sort:
                if sort_order != test.working_sort:
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


class ReferenceRange:

    def __init__(self,
                 minimum: t.Optional[decimal.Decimal] = None,
                 maximum: t.Optional[decimal.Decimal] = None,
                 units: t.Optional[str] = None,
                 kwargs: t.Optional[dict[str, str | int]] = None):
        self.minimum = minimum
        self.maximum = maximum
        self.units = units or None
        self.kwargs = kwargs or {}

    @staticmethod
    def from_map(map_: dict):
        return ReferenceRange(
            decimal.Decimal(map_['minimum']) if 'minimum' in map_ else None,
            decimal.Decimal(map_['maximum']) if 'maximum' in map_ else None,
            str(map_['units']) if 'units' in map_ else None,
            map_['kwargs'] if 'kwargs' in map_ else None
        )


class ElementType(enum.IntFlag):
    COORDINATES = enum.auto()
    PARAMETERS = enum.auto()
    PARENT_METADATA = enum.auto()
    CHILD_METADATA = enum.auto()
    ELEMENT_METADATA = enum.auto()

    RECORD_METADATA = PARENT_METADATA | CHILD_METADATA
    METADATA = PARENT_METADATA | CHILD_METADATA | ELEMENT_METADATA


class CheckerContext:

    def __init__(self,
                 checker: QualityChecker,
                 specific_test_name: str,
                 references: t.Iterable[AnyRef]):
        self.checker: QualityChecker = checker
        self.specific_test_name = specific_test_name
        self.references = list(references)

    def check_review_already_complete(self,
                                      skip_with_final_quality: bool = True,
                                      skip_dubious: bool = False,
                                      skip_empty: bool = False,
                                      skip_flagged_empty: bool = True,
                                      skip_erroneous: bool = True,
                                      skip_bad_structure: bool = True):
        self.checker.check_review_already_complete(
            self.references,
            skip_erroneous=skip_erroneous,
            skip_with_final_quality=skip_with_final_quality,
            skip_dubious=skip_dubious,
            skip_empty=skip_empty,
            skip_flagged_empty=skip_flagged_empty,
            skip_bad_structure=skip_bad_structure
        )

    def set_working_quality(self, working_quality: int):
        for reference in self.references:
            self.checker.set_working_quality(working_quality, reference.ref_object)

    def recommend_for_review(self,
                             quality_flag: int | None = None,
                             subpath: str = "",
                             ref_value: t.Any = None,
                             qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW,
                             message: str | None = None):
        self.checker.recommend_for_review(
            self.specific_test_name,
            self.references,
            quality_flag,
            subpath,
            ref_value,
            qc_result,
            message
        )

@dataclasses.dataclass
class AnyRef:
    path: str
    parent: AnyRef | None

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"<{self.__class__.__name__}:{self.path}>"

    @property
    def ref_object(self) -> ocproc2.BaseRecord | ocproc2.AbstractElement | ocproc2.RecordSet:
        raise NotImplementedError

@dataclasses.dataclass
class ElementRef(AnyRef):
    element: ocproc2.AbstractElement
    element_name: str
    element_type: ElementType

    @property
    def ref_object(self):
        return self.element

@dataclasses.dataclass
class SingleElementRef(ElementRef):
    element: ocproc2.SingleElement


@dataclasses.dataclass
class MultiElementRef(ElementRef):
    element: ocproc2.MultiElement

@dataclasses.dataclass
class RecordSetRef(AnyRef):
    recordset: ocproc2.RecordSet
    recordset_type: str

    @property
    def ref_object(self):
        return self.recordset

@dataclasses.dataclass
class RecordRef(AnyRef):
    record: ocproc2.BaseRecord

    @property
    def ref_object(self):
        return self.record

@dataclasses.dataclass
class ParentRecordRef(RecordRef):
    record: ocproc2.ParentRecord

@dataclasses.dataclass
class ChildRecordRef(RecordRef):
    record: ocproc2.ChildRecord
    recordset_type: str


class QualityChecker(abc.ABC):

    converter: UnitConverter = None

    ALLOW_NEW_QC_RESULT: dict[ocproc2.QCResult, set[ocproc2.QCResult]] = {
        ocproc2.QCResult.PASS: {
            ocproc2.QCResult.FAIL,
            ocproc2.QCResult.SKIP,
            ocproc2.QCResult.ERROR,
            ocproc2.QCResult.MANUAL_REVIEW,
        },
        ocproc2.QCResult.SKIP: {
            ocproc2.QCResult.FAIL,
            ocproc2.QCResult.ERROR,
            ocproc2.QCResult.MANUAL_REVIEW,
        },
        ocproc2.QCResult.FAIL: {
            ocproc2.QCResult.ERROR,
            ocproc2.QCResult.MANUAL_REVIEW,
        },
        ocproc2.QCResult.MANUAL_REVIEW: {
            ocproc2.QCResult.ERROR,
        },
        ocproc2.QCResult.ERROR: {},
    }

    ALLOW_NEW_QUALITY: dict[int | None, set[int]] = {
        None: {0, 1, 2, 3, 4, 5, 7, 9, -1},
        0: {1, 2, 3, 4, 5, 7, 9, -1},
        1: {2, 3, 4, 5, 7, 9, -1},
        5: {2, 3, 4, 5, 7, 9, -1},
        2: {3, 4, 5, 7, 9, -1},
        3: {4, 5, 7, 9, -1},
        4: {5, 7, 9, -1},
        7: {-1},
        9: {-1},
        -1: {},
    }

    SKIP_METADATA: set[str] = {
        "WorkingQuality",
    }

    @injector.construct
    def __init__(self,
                 test_name: str,
                 test_version: str,
                 station_invariant: bool = False,
                 working_sort: str | tuple[str, bool] | None = None,
                 test_tags: list[str] | None = None):
        self._test_name = test_name
        self._station_invariant = station_invariant
        self._test_version = test_version
        self._test_tags = test_tags
        self._working_sort = working_sort
        self._qc_messages: list[ocproc2.QCMessage] = []
        self._qc_result: ocproc2.QCResult = ocproc2.QCResult.PASS
        self._current_record: t.Optional[ParentRecordRef] = None
        self._current_coordinates: t.Optional[dict[str, amath.AnyNumber | AwareDateTime | None]] = None
        self._memory: dict | None = None
        self._rmemory: dict | None = None
        self._log = zrlog.get_logger(f"pipeman.qc_checker.{test_name}")

    @property
    def working_sort(self) -> str | tuple[str, bool] | None:
        return self._working_sort

    @property
    def station_invariant(self) -> bool:
        return self._station_invariant

    @property
    def current_record(self) -> ParentRecordRef:
        if self._current_record is None:
            raise TypeError("current_record is not yet set")
        return self._current_record

    @property
    def current_coordinates(self) -> dict[str, amath.AnyNumber | AwareDateTime | None]:
        if self._current_coordinates is None:
            self._current_coordinates = {}
        return t.cast(dict, self._current_coordinates)

    @property
    def current_latitude(self) -> amath.AnyNumber | None:
        return t.cast(amath.AnyNumber | None, self.current_coordinates.get("Latitude", None))

    @property
    def current_longitude(self) -> amath.AnyNumber | None:
        return t.cast(amath.AnyNumber | None, self.current_coordinates.get("Longitude", None))

    @property
    def current_depth(self) -> amath.AnyNumber | None:
        depth = t.cast(amath.AnyNumber | None, self.current_coordinates.get("Depth", None))
        if depth is None:
            pressure = t.cast(amath.AnyNumber | None, self.current_coordinates.get("Pressure", None))
            latitude = self.current_latitude
            if pressure is not None and latitude is not None:
                import medsutil.seawater as seawater
                self.current_coordinates['Depth'] = depth = seawater.eos80_depth(pressure, latitude)
        return depth

    @property
    def current_pressure(self) -> amath.AnyNumber | None:
        pressure = t.cast(amath.AnyNumber | None, self.current_coordinates.get("Pressure", None))
        if pressure is None:
            depth = t.cast(amath.AnyNumber | None, self.current_coordinates.get("Depth", None))
            latitude = self.current_latitude
            if depth is not None and latitude is not None:
                import medsutil.seawater as seawater
                self.current_coordinates['Pressure'] = pressure = seawater.eos80_pressure(depth, latitude)
        return pressure

    @property
    def current_time(self) -> AwareDateTime | None:
        return t.cast(AwareDateTime | None, self.current_coordinates.get("Time", None))

    def set_coordinates_from_record(self, record: ocproc2.BaseRecord):
        coordinates = self.current_coordinates
        if "Latitude" in record.coordinates:
            try:
                self.require_value(record.coordinates["Latitude"])
                if record.coordinates["Latitude"].is_numeric():
                    coordinates["Latitude"] = record.coordinates["Latitude"].to_numeric("degrees_north")
                else: raise QCSkipReview()
            except QCSkipReview:
                coordinates["Latitude"] = None
        if "Longitude" in record.coordinates:
            try:
                self.require_value(record.coordinates["Longitude"])
                if record.coordinates["Latitude"].is_numeric():
                    coordinates["Longitude"] = record.coordinates["Longitude"].to_numeric("degrees_east")
                else: raise QCSkipReview()
            except QCSkipReview:
                coordinates["Longitude"] = None
        if "Time" in record.coordinates:
            try:
                self.require_value(record.coordinates["Time"])
                if record.coordinates["Time"].is_iso_datetime():
                    coordinates["Time"] = record.coordinates["Time"].to_datetime()
                else: raise QCSkipReview()
            except QCSkipReview:
                coordinates["Time"] = None
        # Pressure and Depth are related. We usually only get one and we calculate the other.
        # We cache the calculation, so we clear the other when we set it
        # The only exception is if we somehow get both - then we want to keep the original value
        # So we need to track if the depth is the real depth
        depth_set: bool = False
        if "Depth" in record.coordinates:
            coordinates['Pressure'] = None
            try:
                self.require_value(record.coordinates["Depth"])
                if record.coordinates["Depth"].is_numeric():
                    coordinates["Depth"] = record.coordinates["Depth"].to_numeric("m")
                    depth_set = True
                else: raise QCSkipReview()
            except QCSkipReview:
                coordinates["Depth"] = None
        if "Pressure" in record.coordinates:
            if not depth_set:
                coordinates['Depth'] = None
            try:
                self.require_value(record.coordinates["Pressure"])
                if record.coordinates["Pressure"].is_numeric():
                    coordinates["Pressure"] = record.coordinates["Pressure"].to_numeric("dbar")
                else: raise QCSkipReview()
            except QCSkipReview:
                coordinates["Pressure"] = None

    @property
    def record_memory(self) -> dict:
        if self._rmemory is None:
            self._rmemory = {}
        return t.cast(dict, self._rmemory)

    @property
    def batch_memory(self) -> dict:
        if self._memory is None:
            self._memory = {}
        return t.cast(dict, self._memory)

    def setup(self): ...

    def run_record_check(self, record: ocproc2.ParentRecord) -> ocproc2.QCTestRunInfo:
        self._current_record = ParentRecordRef(record=record, path="", parent=None)
        self.setup()
        try:
            self.run()
        except QCSkipTest as ex:
            self.update_qc_result(ocproc2.QCResult.SKIP)
            self.add_note(f"test_skipped: {str(ex)}")
            self._log.debug("test skipped", exc_info=True)
        except CodedError as ex:
            if ex.is_transient:
                raise ex
            self.update_qc_result(ocproc2.QCResult.ERROR)
            self.add_note(f"error cause: {ex.__class__.__name__}: {str(ex)}")
            self._log.error("test error", exc_info=True)
        except Exception as ex:
            self.update_qc_result(ocproc2.QCResult.ERROR)
            self.add_note(f"error cause: {ex.__class__.__name__}: {str(ex)}")
            self._log.error("test error", exc_info=True)
        test_run_info = ocproc2.QCTestRunInfo(
            test_name=self._test_name,
            test_version=self._test_version,
            test_tags=self._test_tags,
            test_date=AwareDateTime.utcnow(),
            result=self._qc_result,
            messages=self._qc_messages,
        )
        record.qc_tests.append(test_run_info)
        self.teardown()
        return test_run_info

    def run(self):
        raise NotImplementedError

    def teardown(self):
        self._current_record = None
        self._rmemory = None
        self._qc_result = ocproc2.QCResult.PASS
        self._qc_messages = []

    def crawl_record(self,
                     ref: ParentRecordRef | ChildRecordRef,
                     *,
                     record_cb: t.Callable[[RecordRef], t.Any] | None = None,
                     parent_record_cb: t.Callable[[ParentRecordRef], t.Any] | None = None,
                     child_record_cb: t.Callable[[ChildRecordRef], t.Any] | None = None,
                     element_cb: t.Callable[[ElementRef], t.Any] | None = None,
                     multi_element_cb: t.Callable[[MultiElementRef], t.Any] | None = None,
                     single_element_cb: t.Callable[[SingleElementRef], t.Any] | None = None,
                     recordset_cb: t.Callable[[RecordSetRef], t.Any] | None = None,
                     limit_element_types: ElementType | None = None,
                     limit_subrecord_types: t.Container[str] | None = None,
                     track_coordinates: bool = False):
        if track_coordinates:
            self.set_coordinates_from_record(ref.record)
        if record_cb is not None:
            with self.skip_review_blocker():
                record_cb(ref)
        if parent_record_cb is not None or child_record_cb is not None:
            if isinstance(ref, ParentRecordRef):
                if parent_record_cb is not None:
                    with self.skip_review_blocker():
                        parent_record_cb(ref)
            else:
                if child_record_cb is not None:
                    with self.skip_review_blocker():
                        child_record_cb(ref)
        element_kwargs = {
            "element_cb": element_cb,
            "multi_element_cb": multi_element_cb,
            "single_element_cb": single_element_cb,
        }
        has_element_kwargs = any(x is not None for x in element_kwargs.values())
        if has_element_kwargs:
            for element in self.iterate_on_record_elements(ref, limit_element_types):
                self.crawl_element(
                    element,
                    limit_types=limit_element_types,
                    **element_kwargs
                )

        recordset_kwargs = {
            "recordset_cb": recordset_cb,
            "record_cb": record_cb,
            "child_record_cb": child_record_cb,
            # note: parent_record_cb is only called on the top record, so we don't need it when we recurse
        }
        if has_element_kwargs or (x is not None for x in recordset_kwargs.values()):
            for recordset in self.iterate_on_record_recordsets(ref, limit_subrecord_types):
                self.crawl_recordset(
                    recordset,
                    limit_subrecord_types=limit_subrecord_types,
                    limit_element_types=limit_element_types,
                    track_coordinates=track_coordinates,
                    **element_kwargs,
                    **recordset_kwargs
                )

    def crawl_recordset(self,
                        recordset: RecordSetRef,
                        recordset_cb: t.Callable[[RecordSetRef], t.Any] | None = None,
                        **kwargs):
        if recordset_cb is not None:
            recordset_cb(recordset)
        for child_record in self.iterate_on_recordset_records(recordset):
            self.crawl_record(child_record, recordset_cb=recordset_cb, **kwargs)

    def crawl_element(self,
                      element: SingleElementRef | MultiElementRef,
                      *,
                      element_cb: t.Callable[[ElementRef], t.Any] | None = None,
                      multi_element_cb: t.Callable[[MultiElementRef], t.Any] | None = None,
                      single_element_cb: t.Callable[[SingleElementRef], t.Any] | None = None,
                      limit_types: ElementType | None = None):
        if element_cb is not None:
            with self.skip_review_blocker():
                element_cb(element)
        if isinstance(element, MultiElementRef):
            if multi_element_cb is not None:
                with self.skip_review_blocker():
                    multi_element_cb(element)
            for sub_element in self.iterate_on_element_subelements(element):
                self.crawl_element(sub_element,
                                   element_cb=element_cb,
                                   multi_element_cb=multi_element_cb,
                                   single_element_cb=single_element_cb,
                                   limit_types=limit_types)
        elif single_element_cb is not None:
            with self.skip_review_blocker():
                single_element_cb(element)
        for element in self.iterate_on_element_metadata(element):
            self.crawl_element(element,
                               element_cb=element_cb,
                               multi_element_cb=multi_element_cb,
                               single_element_cb=single_element_cb,
                               limit_types=limit_types)

    def iterate_on_recordset_records(self, recordset: RecordSetRef) -> t.Iterable[ChildRecordRef]:
        yield from self.iterate_on_recordset(recordset.recordset, recordset.recordset_type, recordset)

    def iterate_on_record_recordsets(self, ref: RecordRef, limit_types: t.Container[str] | None = None) -> t.Iterable[RecordSetRef]:
        for subrecord_type, subrecord_sets in ref.record.subrecords.record_sets.items():
            if limit_types is None or subrecord_type in limit_types:
                yield from self.iterate_on_recordset_dict(subrecord_sets, subrecord_type, ref, ref.path.rstrip("/") + f"/subrecords/{subrecord_type}")

    def iterate_on_record_single_elements(self, ref: RecordRef, limit_types: ElementType | None = None) -> t.Iterable[SingleElementRef]:
        for e_ref in self.iterate_on_record_elements(ref, limit_types):
            yield from self.iterate_on_single_elements(e_ref)

    def iterate_on_record_elements(self, ref: RecordRef, limit_types: ElementType | None = None) -> t.Iterable[SingleElementRef | MultiElementRef]:
        if limit_types is None or ElementType.PARAMETERS in limit_types:
            yield from self.iterate_on_element_map(ref.record.parameters, ElementType.PARAMETERS, ref, ref.path.rstrip("/") + "/parameters")
        if limit_types is None or ElementType.COORDINATES in limit_types:
            yield from self.iterate_on_element_map(ref.record.coordinates, ElementType.COORDINATES, ref, ref.path.rstrip("/") + "/coordinates")
        metadata_type = ElementType.PARENT_METADATA if isinstance(ref, ParentRecordRef) else ElementType.CHILD_METADATA
        if limit_types is None or metadata_type in limit_types:
            yield from self.iterate_on_element_map(ref.record.metadata, metadata_type, ref, ref.path.rstrip("/") + "/metadata")

    def iterate_on_single_elements(self, ref: SingleElementRef | MultiElementRef) -> t.Iterable[SingleElementRef]:
        if isinstance(ref, SingleElementRef):
            yield ref
        else:
            for sub_ref in self.iterate_on_element_subelements(ref):
                yield from self.iterate_on_single_elements(sub_ref)

    def iterate_on_element_metadata(self, ref: ElementRef, limit_types: ElementType | None = None) -> t.Iterable[ElementRef]:
        if limit_types is None or ElementType.ELEMENT_METADATA in limit_types:
            yield from self.iterate_on_element_map(ref.element.metadata, ElementType.ELEMENT_METADATA, ref, ref.path.rstrip("/") + "/metadata")

    def iterate_on_recordset(self, recordset: ocproc2.RecordSet, recordset_type: str, parent: AnyRef, parent_path: str | None = None) -> t.Iterable[ChildRecordRef]:
        for idx, record in enumerate(recordset.records.iterate_with_load()):
            yield ChildRecordRef(
                record=record,
                recordset_type=recordset_type,
                path=(parent_path or parent.path).rstrip("/") + f"/{idx}",
                parent=parent
            )

    def iterate_on_recordset_dict(self, recordset_dict: dict[int, ocproc2.RecordSet], recordset_type: str, parent: AnyRef, parent_path: str) -> t.Iterable[RecordSetRef]:
        for idx, record_set in recordset_dict:
            yield RecordSetRef(
                recordset=record_set,
                recordset_type=recordset_type,
                path=parent_path.rstrip("/") + f"/{idx}",
                parent=parent
            )

    def iterate_on_element_subelements(self, element: MultiElementRef) -> t.Iterable[SingleElementRef | MultiElementRef]:
        for idx, sub_element in enumerate(element.element.values()):
            yield self.build_element_ref(
                element=sub_element,
                element_type=element.element_type,
                element_name=element.element_name,
                path=element.path.rstrip("/") + f"/{idx}",
                parent=element,
            )

    def iterate_on_element_map(self, element_map: ocproc2.ElementMap, element_map_name: ElementType, parent: AnyRef, parent_path: str) -> t.Iterable[SingleElementRef | MultiElementRef]:
        for name, element in element_map.items():
            if name in self.SKIP_METADATA:
                continue
            yield self.build_element_ref(
                element=element,
                element_name=name,
                element_type=element_map_name,
                path="/".join((parent_path.rstrip("/"), name)),
                parent=parent
            )

    @t.overload
    def get_record_coordinate_ref(self, ref: RecordRef, coordinate_name: str, create_when_missing: t.Literal[True] = True) -> SingleElementRef | MultiElementRef: ...

    @t.overload
    def get_record_coordinate_ref(self, ref: RecordRef, coordinate_name: str, create_when_missing: t.Literal[False] = False) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    @t.overload
    def get_record_coordinate_ref(self, ref: RecordRef, coordinate_name: str) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    def get_record_coordinate_ref(self,
                                  ref: RecordRef,
                                  coordinate_name: str,
                                  create_when_missing: bool = False) -> t.Optional[SingleElementRef | MultiElementRef]:
        return self._get_element_map_ref(
            element_map=ref.record.coordinates,
            name=coordinate_name,
            element_type=ElementType.COORDINATES,
            parent_path=ref.path.rstrip("/") + f"/coordinates",
            create_when_missing=create_when_missing,
            parent=ref
        )

    @t.overload
    def get_record_parameter_ref(self, ref: RecordRef, parameter_name: str, create_when_missing: t.Literal[True]) -> SingleElementRef | MultiElementRef: ...

    @t.overload
    def get_record_parameter_ref(self, ref: RecordRef, parameter_name: str, create_when_missing: t.Literal[False]) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    @t.overload
    def get_record_parameter_ref(self, ref: RecordRef, parameter_name: str) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    def get_record_parameter_ref(self,
                                  ref: RecordRef,
                                  parameter_name: str,
                                  create_when_missing: bool = False) -> t.Optional[SingleElementRef | MultiElementRef]:
        return self._get_element_map_ref(
            element_map=ref.record.parameters,
            name=parameter_name,
            element_type=ElementType.PARAMETERS,
            parent_path=ref.path.rstrip("/") + f"/parameters",
            create_when_missing=create_when_missing,
            parent=ref
        )

    @t.overload
    def get_record_metadata_ref(self, ref: RecordRef, metadata_name: str, create_when_missing: t.Literal[True]) -> SingleElementRef | MultiElementRef: ...

    @t.overload
    def get_record_metadata_ref(self, ref: RecordRef, metadata_name: str, create_when_missing: t.Literal[False]) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    @t.overload
    def get_record_metadata_ref(self, ref: RecordRef, metadata_name: str) -> t.Optional[SingleElementRef | MultiElementRef]: ...

    def get_record_metadata_ref(self,
                                  ref: RecordRef,
                                  metadata_name: str,
                                  create_when_missing: bool = False) -> t.Optional[SingleElementRef | MultiElementRef]:
        return self._get_element_map_ref(
            element_map=ref.record.metadata,
            name=metadata_name,
            element_type=ElementType.PARENT_METADATA if isinstance(ref, ParentRecordRef) else ElementType.CHILD_METADATA,
            parent_path=ref.path.rstrip("/") + f"/metadata",
            create_when_missing=create_when_missing,
            parent=ref
        )
    
    def get_element_metadata_ref(self,
                                 ref: ElementRef,
                                 metadata_name: str,
                                 create_when_missing: bool = False,) -> t.Optional[SingleElementRef | MultiElementRef]:
        return self._get_element_map_ref(
            element_map=ref.element.metadata,
            name=metadata_name,
            element_type=ElementType.ELEMENT_METADATA,
            parent_path=ref.path.rstrip("/") + f"/metadata",
            create_when_missing=create_when_missing,
            parent=ref
        )
    def _get_element_map_ref(self,
                             element_map: ocproc2.ElementMap,
                             name: str,
                             element_type: ElementType,
                             parent_path: str,
                             create_when_missing: bool,
                             parent: AnyRef) -> t.Optional[SingleElementRef | MultiElementRef]:
        if name not in element_map:
            if create_when_missing:
                element_map[name] = None
            else:
                return None
        return self.build_element_ref(
            element_map[name],
            element_name=name,
            element_type=element_type,
            path=parent_path.rstrip("/") + f"/{name}",
            parent=parent
        )

    def build_element_ref(self, element: ocproc2.AbstractElement, **kwargs) -> SingleElementRef | MultiElementRef:
        if isinstance(element, ocproc2.SingleElement):
            return SingleElementRef(element=element, **kwargs)
        else:
            return MultiElementRef(element=t.cast(ocproc2.MultiElement, element), **kwargs)

    @contextlib.contextmanager
    def skip_review_blocker(self):
        try:
            yield self
        except QCSkipReview as ex:
            self._log.trace("review skipped outside of review block: %s", ex)

    @contextlib.contextmanager
    def review_all(self,
                   review_name: str,
                   refs: t.Iterable[AnyRef],
                   fail_flag: int | None = None,
                   pass_flag: int | None = None,
                   qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Generator[CheckerContext, None, None]:
        ctx = CheckerContext(self, review_name, refs)
        try:
            yield ctx
            self._log.debug("review %s passed on [%s]", review_name, refs)
            if pass_flag is not None:
                for ref in refs:
                    self.set_working_quality(pass_flag, ref.ref_object)
        except QCSkipReview as ex:
            self._log.info("review %s skipped: %s on [%s]", review_name, ex, refs)
        except QCAssertionError as ex:
            self._log.info("review %s failed: %s on [%s]", review_name, ex, refs)
            ctx.recommend_for_review(
                ex.flag_number if ex.flag_number is not None else fail_flag,
                ex.subpath or "",
                ex.ref_value,
                qc_result,
                ex.error_code
            )

    @contextlib.contextmanager
    def review(self,
               review_name: str,
               ref: AnyRef,
               fail_flag: int | None = None,
               pass_flag: int | None = None,
               qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Generator[CheckerContext, None, None]:
        with self.review_all(review_name, [ref], fail_flag, pass_flag, qc_result) as ctx:
            yield ctx

    def check_element_quality(self,
                              element: ocproc2.AbstractElement | ocproc2.BaseRecord | ocproc2.RecordSet | None,
                              skip_with_final_quality: bool = True,
                              skip_dubious: bool = False,
                              skip_empty: bool = False,
                              skip_flagged_empty: bool = True,
                              skip_erroneous: bool = True,
                              skip_bad_structure: bool = True):
        if element is None:
            self.skip_review("element_is_none")
        else:
            existing_quality = element.metadata.best("Quality", coerce=int, default=0)
            if skip_with_final_quality and existing_quality != 0:
                self.skip_review("element_has_final_quality")

            working_quality = element.metadata.best("WorkingQuality", coerce=int, default=0)
            if skip_flagged_empty and existing_quality == 9 or working_quality == 9:
                self.skip_review("element_flagged_empty")
            if skip_erroneous and existing_quality == 4 or working_quality == 4:
                self.skip_review("element_flagged_erroneous")
            if skip_dubious and existing_quality == 3 or working_quality == 3:
                self.skip_review("element_flagged_dubious")
            if skip_bad_structure and existing_quality == -1 or working_quality == -1:
                self.skip_review("element_has_bad_structure")

            if skip_empty and hasattr(element, 'is_empty') and element.is_empty():
                self.skip_review("element_empty")

    def check_review_already_complete(self,
                                      references: t.List[AnyRef] | AnyRef | ocproc2.AbstractElement | ocproc2.RecordSet | ocproc2.BaseRecord,
                                      **kwargs):
        if isinstance(references, list):
            any_passed: bool = False
            any_skipped: bool = False
            for ref in references:
                try:
                    self.check_review_already_complete(ref.ref_object, **kwargs)
                    any_passed = True
                except QCSkipReview:
                    any_skipped = True
            if any_skipped and not any_passed:
                self.skip_review("all_completed")
        elif isinstance(references, AnyRef):
            self.check_element_quality(references.ref_object, **kwargs)
        else:
            self.check_element_quality(references, **kwargs)

    def set_working_quality(self,
                            working_quality: int,
                            element: ocproc2.AbstractElement | ocproc2.RecordSet | ocproc2.BaseRecord):
        existing_quality = element.metadata.best("WorkingQuality", default=None, coerce=int)
        if working_quality in self.ALLOW_NEW_QUALITY[existing_quality]:
            element.metadata["WorkingQuality"] = working_quality

    def recommend_for_review(self,
                             specific_test_name: str,
                             refs: AnyRef | t.Iterable[AnyRef],
                             quality_flag: int | None = None,
                             subpath: str = "",
                             ref_value: t.Any = None,
                             qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW,
                             message: str | None = None):
        if not isinstance(refs, AnyRef):
            for e in refs:
                self.recommend_for_review(specific_test_name, e, quality_flag, subpath, ref_value, qc_result, message)
        else:
            element_path = refs.path
            element = refs.ref_object
            if subpath:
                element_path = f"{refs.path.rstrip("/")}/{subpath.strip("/")}"
                element = element.find_child(subpath)
            if quality_flag is not None and element is not None and isinstance(element, (ocproc2.BaseRecord, ocproc2.RecordSet, ocproc2.AbstractElement)):
                self.set_working_quality(quality_flag, element)
            if message is not None:
                self.add_qc_message(message, element_path, ref_value)
            if qc_result is not None:
                self.update_qc_result(qc_result)

    def skip_remaining_tests(self, reason: str):
        raise QCSkipTest(reason)

    def skip_review(self, reason: str):
        raise QCSkipReview(reason)

    def update_qc_result(self, new_result: ocproc2.QCResult):
        if new_result in self.ALLOW_NEW_QC_RESULT[self._qc_result]:
            self._qc_result = new_result

    def add_qc_message(self,
                       msg: str,
                       path: str | list[str],
                       ref_value: t.Any = None,
                       specific_test_name: str = None):
        # TODO: can we put specific_test_name into the message somewhere?
        self._qc_messages.append(ocproc2.QCMessage(
            msg,
            path,
            ref_value
        ))

    def add_note(self, msg: str):
        self.current_record.record.add_history_entry(
            msg,
            self._test_name,
            self._test_version,
            "",
        )

    def extract_good_values(self, element: SingleElementRef | MultiElementRef | None, skip_dubious: bool = True) -> t.Iterable[SingleElementRef]:
        if element is not None:
            for element_sref in self.iterate_on_single_elements(element):
                with self.skip_review_blocker():
                    self.require_value(element_sref.element, skip_dubious)
                    yield element_sref

    def require_value(self,
                      value: ocproc2.AbstractElement | None,
                      skip_dubious: bool = True) -> t.TypeGuard[ocproc2.AbstractElement]:
        self.check_element_quality(value,
                                   skip_with_final_quality=False,
                                   skip_empty=True,
                                   skip_flagged_empty=True,
                                   skip_erroneous=True,
                                   skip_bad_structure=True,
                                   skip_dubious=skip_dubious)
        return True

    @staticmethod
    def review_cb(test_name: str,
                  skip_with_final_quality: bool = True,
                  skip_dubious: bool = False,
                  skip_empty: bool = False,
                  skip_flagged_empty: bool = True,
                  skip_erroneous: bool = True,
                  skip_bad_structure: bool = True,
                  fail_flag: int | None = None,
                  pass_flag: int | None = None,
                  qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Callable[[QCMethodProtocol], QCMethodProtocol]:
        def _outer(cb: QCMethodProtocol) -> QCMethodProtocol:
            @functools.wraps(cb)
            def _inner(self: QualityChecker, ref: AnyRef, *args, **kwargs) -> t.Any:
                with self.review(test_name, ref, fail_flag=fail_flag, pass_flag=pass_flag, qc_result=qc_result) as ctx:
                    ctx.check_review_already_complete(skip_empty=skip_empty,
                                                      skip_erroneous=skip_erroneous,
                                                      skip_dubious=skip_dubious,
                                                      skip_flagged_empty=skip_flagged_empty,
                                                      skip_with_final_quality=skip_with_final_quality,
                                                      skip_bad_structure=skip_bad_structure)
                    return cb(self, ref, *args, **kwargs)

            return _inner

        return _outer

    def report_qc_error(self,
                        msg: str,
                        flag: int | None = None,
                        subpath: t.Optional[str] = None,
                        ref_value: t.Any = None):
        raise QCAssertionError(
            error_code=msg,
            flag_number=flag,
            subpath=subpath,
            ref_value=ref_value
        )

    def assert_true(self, a: t.Any, msg: str | None = None, **kwargs) -> bool:
        if not a:
            self.report_qc_error(msg or "not_true", **kwargs)
        return True

    def assert_false(self, a: t.Any, msg: str | None = None, **kwargs) -> bool:
        if a:
            self.report_qc_error(msg or "not_false", **kwargs)
        return True

    def assert_is_none(self, a: t.Any, msg:str | None = None, **kwargs) -> t.TypeGuard[None]:
        if a is not None:
            self.report_qc_error(msg or "not_none", **kwargs)
        return True

    def assert_is_not_none[T](self, a: T | None, msg: str | None = None, **kwargs) -> t.TypeGuard[T]:
        if a is None:
            self.report_qc_error(msg or "is_none", **kwargs)
        return True

    def assert_element_not_empty(self, element: ocproc2.SingleElement, msg: str | None = None, **kwargs):
        if element.is_empty():
            self.report_qc_error(msg or "is_empty", **kwargs)

    def assert_element_is_number(self, element: ocproc2.SingleElement, msg: str | None = None, **kwargs):
        if not element.is_numeric():
            self.report_qc_error(msg or "not_numeric", **kwargs)

    def assert_in_reference_range(self, element: ocproc2.SingleElement, ref_range: ReferenceRange, msg: str | None = None, **kwargs):
        if ref_range.minimum is None and ref_range.maximum is None:
            return
        number = element.to_numeric(ref_range.units)
        if ref_range.maximum is None:
            self.assert_greater_or_close(number, t.cast(amath.AnyNumber, ref_range.minimum), msg=msg or "outside_ref_range", **kwargs)
        elif ref_range.minimum is None:
            self.assert_less_or_close(number, t.cast(amath.AnyNumber, ref_range.maximum), msg=msg or "outside_ref_range", **kwargs)
        else:
            self.assert_between(number, t.cast(amath.AnyNumber, ref_range.minimum), t.cast(amath.AnyNumber, ref_range.maximum), msg=msg or "outside_ref_range", **kwargs)

    def assert_between(self, a: amath.AnyNumber, min_value: amath.AnyNumber, max_value: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not amath.between(min_value, a, max_value):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = f"{min_value} TO {max_value}"
            self.report_qc_error(msg or "not_between", **kwargs)

    def assert_in(self, element: t.Any, collection: collections.abc.Container, msg: str | None = None, **kwargs) -> bool:
        if element not in collection:
            self.report_qc_error(msg or "not_in", **kwargs)
        return True

    def assert_is_instance(self, v: t.Any, types: tuple[type, ...] | type, msg: str | None = None, **kwargs) -> bool:
        if not isinstance(v, types):
            if not kwargs.get("ref_value", None):
                if isinstance(types, tuple):
                    kwargs['ref_value'] = kwargs.get('ref_value', ";".join(str(x.__name__ for x in types)))
                else:
                    kwargs['ref_value'] = kwargs.get('ref_value', ";".join(str(types.__name__)))
            self.report_qc_error(msg or "is_not_type", **kwargs)
        return True

    def assert_is_not_instance(self, v: t.Any, types: tuple[type, ...] | type, msg: str | None = None, **kwargs):
        if isinstance(v, types):
            if not kwargs.get("ref_value", None):
                if isinstance(types, tuple):
                    kwargs['ref_value'] = kwargs.get('ref_value', ";".join(str(x.__name__ for x in types)))
                else:
                    kwargs['ref_value'] = kwargs.get('ref_value', ";".join(str(types.__name__)))
            self.report_qc_error(msg or "is_type", **kwargs)
        return True

    def assert_greater_or_close(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not (amath.gt(a, b) or amath.is_close(a, b)):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_greater_or_close", **kwargs)

    def assert_less_or_close(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not (amath.lt(a, b) or amath.is_close(a, b)):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_not_equal(self, a: t.Any, b: t.Any, msg: str | None = None, **kwargs):
        if a == b:
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_equal", **kwargs)


class DeepDiveChecker(QualityChecker):

    LIMIT_SUBRECORD_TYPES: t.Container[str] | None = None
    LIMIT_ELEMENT_TYPES: ElementType | None = None
    TRACK_COORDINATES: bool = False

    element_check: t.Callable[[ElementRef], t.Any] | None = None
    parent_record_check: t.Callable[[ParentRecordRef], t.Any] | None = None
    multi_element_check: t.Callable[[MultiElementRef], t.Any] | None = None
    single_element_check: t.Callable[[SingleElementRef], t.Any] | None = None
    child_record_check: t.Callable[[ChildRecordRef], t.Any] | None = None
    record_check: t.Callable[[RecordRef], t.Any] | None = None
    recordset_check: t.Callable[[RecordSetRef], t.Any] | None = None

    def run(self):
        self.crawl_record(
            self.current_record,
            element_cb=self.element_check,
            single_element_cb=self.single_element_check,
            multi_element_cb=self.multi_element_check,
            parent_record_cb=self.parent_record_check,
            child_record_cb=self.child_record_check,
            record_cb=self.record_check,
            recordset_cb=self.recordset_check,
            limit_element_types=self.LIMIT_ELEMENT_TYPES,
            limit_subrecord_types=self.LIMIT_SUBRECORD_TYPES,
            track_coordinates=self.TRACK_COORDINATES
        )

review = QualityChecker.review_cb
