import abc
import collections.abc
import contextlib
import datetime
import functools
import typing as t

import zrlog

import medsutil.math as amath
from medsutil import ocproc2 as ocproc2
from medsutil.awaretime import AwareDateTime
from medsutil.cached import CachedObjectMixin
from medsutil.exceptions import CodedError
from medsutil.math import _functions
from medsutil.ocproc2.operations import ChangeQuality
from medsutil.ocproc2.refs import ElementType, AnyRef, ElementRef, SingleElementRef, MultiElementRef, \
    RecordSetRef, RecordRef, ParentRecordRef, ChildRecordRef, RecordCrawler
from medsutil.ocproc2.util import QualityError, CoordinateTracker, check_quality, RequiredQuality, Quality, \
    set_working_quality, check_any_of_quality
from medsutil.ocproc_math import extract_parameter_value
from medsutil.units import UnitConverter
from autoinject import injector

from nodb.interface import NODBInstance
from nodb.observations import NODBPlatform, NODBWorkingRecord, NODBObservationData, NODBObservation, NODBSourceFile

if t.TYPE_CHECKING:
    from pipeman.programs.qc.references import ReferenceRange
    from medsutil.ocproc2.util import ObjectWithMetadata
    class QCMethodProtocol[R: AnyRef](t.Protocol):
        def __call__(_, self: QualityController, ref: R, *args, **kwargs) -> t.Any:
            ...



class QCException(Exception): ...

class QCComplete(QCException): ...

class QCSkipReview(QCException): ...

class QCSkipTest(QCException): ...

class QCPassTest(QCException): ...

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


class QualityController(abc.ABC):

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

    SKIP_METADATA: set[str] = {
        "WorkingQuality",
    }

    @injector.construct
    def __init__(self,
                 test_name: str,
                 test_version: str,
                 station_invariant: bool = False,
                 working_sort: str | tuple[str, bool] | None = None,
                 test_tags: list[str | None] | None = None,
                 searcher_cls: type | None = None):
        self._test_name = test_name
        self._station_invariant = station_invariant
        self._test_version = test_version
        self._test_tags = set(x for x in test_tags if x is not None) if test_tags else set()
        self._working_sort = working_sort
        self._qc_messages: list[ocproc2.QCMessage] = []
        self._qc_result: ocproc2.QCResult = ocproc2.QCResult.PASS
        self._reviews_passed = 0
        self._reviews_failed = 0
        self._reviews_skipped = 0
        self._current_record: t.Optional[ParentRecordRef] = None
        self._coordinates: t.Optional[CoordinateTracker] = None
        self._memory: dict | None = None
        self._rmemory: dict | None = None
        self._log = zrlog.get_logger(f"pipeman.qc_checker.{test_name}")
        self._db = None
        self._searcher = None
        self._searcher_cls = searcher_cls or RealPlatformSearcher
        self._set_qc_flag: int = 0
        self._source_file_uuid: str | None = None
        self._source_file_date: datetime.date | None = None

    @property
    def searcher(self) -> PlatformSearcher:
        if self._searcher is None:
            self._searcher = self._searcher_cls(self._db)
        return t.cast(PlatformSearcher, self._searcher)

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
    def current_coordinates(self) -> CoordinateTracker:
        if self._coordinates is None:
            self._coordinates = CoordinateTracker()
        return t.cast(CoordinateTracker, self._coordinates)

    @property
    def current_latitude(self) -> amath.AnyNumber | None:
        return self.current_coordinates.latitude

    @property
    def current_longitude(self) -> amath.AnyNumber | None:
        return self.current_coordinates.longitude

    @property
    def current_depth(self) -> amath.AnyNumber | None:
        return self.current_coordinates.depth

    @property
    def current_pressure(self) -> amath.AnyNumber | None:
        return self.current_coordinates.pressure

    @property
    def current_time(self) -> AwareDateTime | None:
        return self.current_coordinates.time

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

    def set_coordinates_from_record(self, record: ocproc2.BaseRecord):
        self.current_coordinates.update_from_record(record)

    def set_qc_flag(self, flag: int):
        self._set_qc_flag = self._set_qc_flag | flag

    def setup(self): ...

    def get_current_platform(self, require_good: bool = True) -> NODBPlatform | None:
        platform_id = self.get_current_platform_id(require_good)
        if platform_id is not None:
            return self.searcher.find_by_uuid(platform_id)
        return None

    def get_current_platform_id(self, require_good: bool = True) -> str | None:
        current = self.current_record.record
        if require_good:
            self.require_quality(current.metadata.get("CNODCPlatform"))
        if current.metadata.has_value('CNODCPlatform'):
            platform = current.metadata['CNODCPlatform']
            if platform.is_empty() or platform.quality in (4, 9):
                return None
            return platform.to_string()
        return None

    @property
    def db(self) -> NODBInstance:
        if self._db is None:
            raise ValueError("Invalid time to call this")
        else:
            return self._db

    def run_record_check(self,
                         record: ocproc2.ParentRecord,
                         db: NODBInstance,
                         qc_flags: int = 0,
                         source_file_uuid: str | None = None,
                         source_file_date: datetime.date | None = None) -> tuple[ocproc2.QCTestRunInfo, int]:
        self._source_file_uuid = source_file_uuid
        self._source_file_date = source_file_date
        self._set_qc_flag = qc_flags
        record.add_processed_by(self._test_name, self._test_version, '')
        self._current_record = ParentRecordRef(record=record)
        self._db = db
        self.setup()
        try:
            self.run()
            if self._reviews_passed == 0 and self._reviews_failed == 0:
                self.update_qc_result(ocproc2.QCResult.SKIP)
        except QCSkipTest as ex:
            self.update_qc_result(ocproc2.QCResult.SKIP)
            self.add_note(f"test_skipped: {str(ex)}")
            self._log.debug("test skipped", exc_info=True)
        except QCPassTest as ex:
            self.update_qc_result(ocproc2.QCResult.PASS)
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
            test_tags=list(self._test_tags),
            test_date=AwareDateTime.utcnow(),
            result=self._qc_result,
            messages=self._qc_messages,
            notes=f"passed={self._reviews_passed};failed={self._reviews_failed};skipped={self._reviews_skipped}",
        )
        record.qc_tests.append(test_run_info)
        self.teardown()
        return test_run_info, self._set_qc_flag

    def run(self):
        raise NotImplementedError

    def teardown(self):
        self._current_record = None
        self._rmemory = None
        self._qc_result = ocproc2.QCResult.PASS
        self._qc_messages = []
        self._reviews_passed = 0
        self._reviews_failed = 0
        self._reviews_skipped = 0

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
                   qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Generator[
        CheckerContext, None, None]:
        ctx = CheckerContext(self, review_name, refs)
        try:
            yield ctx
            self._log.debug("review %s passed on [%s]", review_name, refs)
            self._reviews_passed += 1
            if pass_flag is not None:
                for ref in refs:
                    self.set_working_quality(pass_flag, ref)
        except QCSkipReview as ex:
            self._log.info("review %s skipped: %s on [%s]", review_name, ex, refs)
            self._reviews_skipped += 1
        except QCAssertionError as ex:
            self._log.info("review [%s] failed: [%s][ref: %s] on [%s]", review_name, ex.error_code, ex.ref_value, refs)
            self._reviews_failed += 1
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
               qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Generator[
        CheckerContext, None, None]:
        with self.review_all(review_name, [ref], fail_flag, pass_flag, qc_result) as ctx:
            yield ctx

    def check_quality(self,
                      element: ObjectWithMetadata | None,
                      required_quality: RequiredQuality,
                      msg: str | None = None):
        try:
            check_quality(element, required_quality)
        except QualityError as ex:
            self.skip_review(msg or str(ex))

    def check_review_already_complete(self,
                                      references: t.List[AnyRef] | AnyRef | ObjectWithMetadata,
                                      required_quality: RequiredQuality = RequiredQuality.QC_INCOMPLETE):
        try:
            if isinstance(references, list):
                check_any_of_quality([r.ref_object for r in references], required_quality)
            elif isinstance(references, AnyRef):
                check_quality(references.ref_object, required_quality)
            else:
                check_quality(references, required_quality)
        except (QualityError, ExceptionGroup) as ex:
            self.skip_review(str(ex))

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
            if subpath:
                element_path = f"{refs.path.rstrip("/")}/{subpath.strip("/")}"
            if quality_flag is not None:
                self.set_working_quality(quality_flag, element_path)
            if message is not None:
                self.add_qc_message(message, element_path, ref_value)
            if qc_result is not None:
                self.update_qc_result(qc_result)

    def set_working_quality(self,
                            working_quality: int,
                            ref: AnyRef | str):
        action = ChangeQuality(
            path=ref.path if isinstance(ref, AnyRef) else ref,
            new_flag=working_quality,
            source_name=self._test_name,
            source_version=self._test_version
        )
        action.apply(self.current_record.record)

    def skip_entire_test(self, reason: str):
        raise QCSkipTest(reason)

    def skip_review(self, reason: str):
        raise QCSkipReview(reason)

    def qc_pass(self):
        raise QCPassTest()

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

    def require_quality(self,
                        value: ocproc2.AbstractElement | None,
                        required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE,
                        msg: str | None = None) -> t.TypeGuard[ocproc2.AbstractElement]:
        self.check_quality(value, required_quality, msg)
        return True

    @staticmethod
    def review_cb(test_name: str,
                  required_quality: RequiredQuality = RequiredQuality.QC_INCOMPLETE,
                  fail_flag: int | None = None,
                  pass_flag: int | None = None,
                  qc_result: ocproc2.QCResult | None = ocproc2.QCResult.MANUAL_REVIEW) -> t.Callable[[QCMethodProtocol], QCMethodProtocol]:
        def _outer(cb: QCMethodProtocol) -> QCMethodProtocol:
            @functools.wraps(cb)
            def _inner(self: QualityController, ref: AnyRef, *args, **kwargs) -> t.Any:
                with self.review(test_name, ref, fail_flag=fail_flag, pass_flag=pass_flag, qc_result=qc_result) as ctx:
                    ctx.check_review_already_complete(required_quality)
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

    def extract_all_keyed_parameters(self, *records: RecordRef, include_parameters: t.Container[str] | None = None) -> t.Iterable[tuple[SingleElementRef | None, ...]]:
        param_names = set()
        for record in records:
            if include_parameters is not None:
                param_names.update(
                    k
                    for k in record.record.parameters.keys()
                    if k in include_parameters
                )
            else:
                param_names.update(record.record.parameters.keys())
        for param_name in param_names:
            yield from self.group_by_sensor_rank(
                *(
                    record.record.parameters.get(param_name, None)
                    for record in records
                )
            )

    def group_by_sensor_rank(self,
                             *elements: SingleElementRef | MultiElementRef | None) -> t.Iterable[tuple[SingleElementRef | None, ...]]:
        keyed_elements = []
        keys = set()
        for element in elements:
            keyed = element.values_keyed_for_sensor_rank() if element is not None else {}
            keyed_elements.append(keyed)
            keys.update(keyed.keys())
        for key in keys:
            yield tuple(
                by_key[key] if key in by_key else None
                for by_key in keyed_elements
            )


    def extract_good_values(self,
                            element_ref: SingleElementRef | MultiElementRef | None,
                            required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE) -> t.Iterable[SingleElementRef]:
        if element_ref is not None:
            for element_sref in element_ref.single_element_refs():
                try:
                    self.require_quality(element_sref.element, required_quality=required_quality)
                    yield element_sref
                except QCSkipReview:
                    ...

    def extract_parameter_values(self, *elements: SingleElementRef, units: str | None = None, required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE) -> tuple[amath.AnyNumber | None, ...]:
        for element in elements:
            if units is None:
                units = element.element.metadata.best("Units", coerce=str, default=None)
            if units is not None:
                break
        return tuple(
            self.extract_parameter_value(element, units, required_quality, self.current_time)
            for element in elements
        )

    def extract_parameter_value(self,
                                ref: SingleElementRef,
                                units: str | None = None,
                                required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE,
                                obs_date: AwareDateTime | None = None) -> amath.AnyNumber | None:
        return extract_parameter_value(ref.element_name, ref.element, units, required_quality, obs_date if obs_date is not None else self.current_time)

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

    def assert_between(self,
                       a: amath.AnyNumber,
                       min_value: amath.AnyNumber,
                       max_value: amath.AnyNumber,
                       msg: str | None = None,
                       add_ref: bool = True,
                       **kwargs):
        if not _functions.between(min_value, a, max_value):
            if add_ref and not kwargs.get("ref_value", None):
                kwargs["ref_value"] = f"{min_value} TO {max_value}"
            self.report_qc_error(msg or "not_between", **kwargs)

    def assert_in(self,
                  element: t.Any,
                  collection: collections.abc.Container,
                  msg: str | None = None,
                  add_ref: bool = False,
                  **kwargs) -> bool:
        if element not in collection:
            if add_ref and isinstance(collection, t.Iterable):
                if not kwargs.get("ref_value", None):
                    kwargs["ref_value"] = collection
            self.report_qc_error(msg or "not_in", **kwargs)
        return True

    def assert_is_instance(self, v: t.Any, types: tuple[type, ...] | type, msg: str | None = None, **kwargs) -> bool:
        if not isinstance(v, types):
            if not kwargs.get("ref_value", None):
                if isinstance(types, tuple):
                    kwargs['ref_value'] = ";".join(str(x.__name__ for x in types))
                else:
                    kwargs['ref_value'] = str(types.__name__)
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

    def assert_greater_or_close(self,
                                a: amath.AnyNumber,
                                b: amath.AnyNumber,
                                msg: str | None = None,
                                rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                                abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                                **kwargs):
        if not amath.gte(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_greater_or_close", **kwargs)

    def assert_not_nan(self, a: amath.AnyNumber, msg: str | None = None, **kwargs):
        if amath.is_nan(a):
            self.report_qc_error(msg or 'is_nan', **kwargs)

    def assert_less_or_close(self,
                             a: amath.AnyNumber,
                             b: amath.AnyNumber,
                             msg: str | None = None,
                             rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                             abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                             **kwargs):
        if not amath.lte(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_greater(self,
                       a: amath.AnyNumber,
                       b: amath.AnyNumber,
                       msg: str | None = None,
                       rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                       abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                       **kwargs):
        if not amath.gt(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_greater_or_close", **kwargs)

    def assert_less(self,
                    a: amath.AnyNumber,
                    b: amath.AnyNumber,
                    msg: str | None = None,
                    rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                    abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                    **kwargs):
        if not amath.lt(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_close(self,
                     a: amath.AnyNumber,
                     b: amath.AnyNumber,
                     msg: str | None = None,
                     rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                     abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                     **kwargs):
        if not amath.is_close(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_close", **kwargs)

    def assert_not_close(self,
                         a: amath.AnyNumber,
                         b: amath.AnyNumber,
                         msg: str | None = None,
                         rel_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_REL_TOL,
                         abs_tol: amath.BasicNumber | amath.NumberString = amath.DEFAULT_ABS_TOL,
                         **kwargs):
        if amath.is_close(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "too_close", **kwargs)

    def assert_equal(self, a: t.Any, b: t.Any, msg: str | None = None, **kwargs):
        if a != b:
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "equal", **kwargs)

    def assert_not_equal(self, a: t.Any, b: t.Any, msg: str | None = None, **kwargs):
        if a == b:
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_equal", **kwargs)


class DeepDiveChecker(QualityController):

    LIMIT_SUBRECORD_TYPES: t.Container[str] | None = None
    LIMIT_ELEMENT_TYPES: ElementType | None = None
    TRACK_COORDINATES: bool = False
    SKIP_ELEMENTS: t.Container[str] | None = ["WorkingQuality"]

    element_check: t.Callable[[ElementRef], t.Any] | None = None
    parent_record_check: t.Callable[[ParentRecordRef], t.Any] | None = None
    multi_element_check: t.Callable[[MultiElementRef], t.Any] | None = None
    single_element_check: t.Callable[[SingleElementRef], t.Any] | None = None
    child_record_check: t.Callable[[ChildRecordRef], t.Any] | None = None
    record_check: t.Callable[[RecordRef], t.Any] | None = None
    recordset_check: t.Callable[[RecordSetRef], t.Any] | None = None

    def run(self):
        crawler = RecordCrawler(
            element_cb=self._wrap_element_callback(self.element_check),
            single_element_cb=self._wrap_element_callback(self.single_element_check),
            multi_element_cb=self._wrap_element_callback(self.multi_element_check),
            parent_record_cb=self._wrap_callback(self.parent_record_check),
            child_record_cb=self._wrap_callback(self.child_record_check),
            record_cb=self._wrap_record_callback(self.record_check),
            recordset_cb=self._wrap_callback(self.recordset_check),
            limit_element_types=self.LIMIT_ELEMENT_TYPES,
            limit_subrecord_types=self.LIMIT_SUBRECORD_TYPES
        )
        crawler.crawl_record(self.current_record)

    def _wrap_element_callback(self, cb):
        skip = self.SKIP_ELEMENTS
        if cb is None:
            return None
        elif skip is not None:
            @functools.wraps(cb)
            def _inner(ref: ElementRef, *args, **kwargs):
                # Never validate these
                if ref.element_name in skip:
                    return None
                with self.skip_review_blocker():
                    return cb(ref, *args, **kwargs)
            return _inner
        else:
            return self._wrap_callback(cb)


    def _wrap_record_callback(self, cb):
        if not self.TRACK_COORDINATES:
            return self._wrap_callback(cb)
        elif cb is not None:
            @functools.wraps(cb)
            def _inner(ref, *args, **kwargs):
                self._update_coordinates(ref)
                with self.skip_review_blocker():
                    return cb(ref, *args, **kwargs)
            return _inner
        else:
            return self._update_coordinates

    def _update_coordinates(self, ref: RecordRef):
        self.set_coordinates_from_record(ref.record)

    def _wrap_callback(self, cb):
        if cb is None:
            return cb
        @functools.wraps(cb)
        def _inner(*args, **kwargs):
            with self.skip_review_blocker():
                return cb(*args, **kwargs)
        return _inner




review = QualityController.review_cb


class CheckerContext:

    def __init__(self,
                 checker: QualityController,
                 specific_test_name: str,
                 references: t.Iterable[AnyRef]):
        self.checker: QualityController = checker
        self.specific_test_name = specific_test_name
        self.references = list(references)

    def check_review_already_complete(self, required_quality: RequiredQuality = RequiredQuality.QC_INCOMPLETE):
        self.checker.check_review_already_complete(
            self.references,
            required_quality
        )

    def set_working_quality(self, working_quality: int):
        for reference in self.references:
            self.checker.set_working_quality(working_quality)

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


class ProfileChecker(DeepDiveChecker):

    LIMIT_SUBRECORD_TYPES: set[str] = {"PROFILE"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._profile_memory: dict | None = None

    @property
    def profile_memory(self) -> dict:
        if self._profile_memory is None:
            self._profile_memory = {}
        return t.cast(dict, self._profile_memory)

    def recordset_check(self, ref: RecordSetRef):
        if ref.recordset_type != "PROFILE":
            return
        profile = [ record_ref for record_ref in ref.record_refs() ]
        try:
            with self.skip_review_blocker():
                self.profile_check(profile, ref)
        finally:
            self._profile_memory = None

    def profile_check(self, profile: list[ChildRecordRef], recordset_ref: RecordSetRef):
        for ref in profile:
            self._update_coordinates(ref)
            with self.skip_review_blocker():
                self.level_check(ref)

    def level_check(self, ref: ChildRecordRef):
        ...


class PlatformSearcher(t.Protocol):

    def __init__(self, db: NODBInstance): ...

    def find_by_uuid(self, platform_uuid: str) -> NODBPlatform | None:
        ...

    def search(self,
               *,
               platform_id: str | None = None,
               platform_name: str | None = None,
               wigos_id: str | None = None,
               wmo_id: str | None = None,
               in_service_time: AwareDateTime | None = None) -> t.Iterable[NODBPlatform]:
        ...

    def geosearch_working_records(self,
                                  platform_uuid: str,
                                  start_time: AwareDateTime,
                                  end_time: AwareDateTime,
                                  min_latitude: amath.BasicNumber,
                                  max_latitude: amath.BasicNumber,
                                  min_longitude: amath.BasicNumber,
                                  max_longitude: amath.BasicNumber) -> t.Iterable[NODBWorkingRecord]:
        ...

    def geosearch_observations(self,
                               platform_uuid: str,
                               start_time: AwareDateTime,
                               end_time: AwareDateTime,
                               min_latitude: amath.BasicNumber,
                               max_latitude: amath.BasicNumber,
                               min_longitude: amath.BasicNumber,
                               max_longitude: amath.BasicNumber) -> t.Iterable[NODBObservationData]:
        ...

    def recent_working_records(self,
                               platform_id: str,
                               start_time: AwareDateTime,
                               end_time: AwareDateTime) -> t.Iterable[NODBWorkingRecord]:
        ...

    def recent_observations(self,
                            platform_id: str,
                            start_time: AwareDateTime,
                            end_time: AwareDateTime) -> t.Iterable[NODBObservationData]:
        ...

    def record_exists(self, obs_date: str, obs_uuid: str) -> bool:
        ...

    def is_source_file_replacement(self,
                                   old_uuid: str | None,
                                   old_date: datetime.date | str | None,
                                   new_uuid: str | None,
                                   new_date: datetime.date | str | None) -> bool:
        ...

class RealPlatformSearcher(CachedObjectMixin):

    def __init__(self, db: NODBInstance):
        self._db = db
        super().__init__()

    def find_by_uuid(self, platform_uuid: str) -> NODBPlatform | None:
        return NODBPlatform.find_by_uuid(self._db, platform_uuid)

    def search(self, **kwargs) -> t.Iterable[NODBPlatform]:
        yield from NODBPlatform.search(self._db, **kwargs)

    def geosearch_working_records(self, **kwargs) -> t.Iterable[NODBWorkingRecord]:
        yield from NODBWorkingRecord.search(self._db, **kwargs)

    def geosearch_observations(self, **kwargs) -> t.Iterable[NODBObservationData]:
        for obs in NODBObservation.search(self._db, **kwargs, key_only=True):
            obs_data = obs.find_observation_data(self._db)
            if obs_data is not None:
                yield obs_data

    def record_exists(self, obs_date: str, obs_uuid: str) -> bool:
        if NODBObservationData.find_by_uuid(self._db, obs_uuid, obs_date, key_only=True) is not None:
            return True
        if NODBWorkingRecord.find_by_uuid(self._db, obs_uuid, key_only=True) is not None:
            return True
        return False

    def recent_working_records(self,
                               platform_id: str,
                               start_time: AwareDateTime,
                               end_time: AwareDateTime) -> t.Iterable[NODBWorkingRecord]:
        yield from NODBWorkingRecord.search(
            self._db,
            platform_uuid=platform_id,
            start_time=start_time,
            end_time=end_time
        )

    def recent_observations(self,
                            platform_id: str,
                            start_time: AwareDateTime,
                            end_time: AwareDateTime) -> t.Iterable[NODBObservationData]:
        for obs in NODBObservation.search(
            self._db,
            platform_uuid=platform_id,
            start_time=start_time,
            end_time=end_time,
            key_only=True
        ):
            obs_data = obs.find_observation_data(self._db)
            if obs_data is not None:
                yield obs_data

    def is_source_file_replacement(self,
                                   old_uuid: str | None,
                                   old_date: datetime.date | str | None,
                                   new_uuid: str | None,
                                   new_date: datetime.date | str | None) -> bool:

        if old_uuid is None or old_date is None or new_uuid is None or new_date is None:
            return False
        return self._with_cache(
            'is_replacement',
            self._is_source_file_replacement,
            old_uuid, old_date, new_uuid, new_date,
            cache_parameters=[old_uuid, old_date, new_uuid, new_date]
        )

    def _is_source_file_replacement(self,
                                   old_uuid: str,
                                   old_date: datetime.date | str,
                                   new_uuid: str,
                                   new_date: datetime.date | str) -> bool:
        file = NODBSourceFile.find_by_uuid(self._db, new_uuid, new_date)
        if file is None:
            return False
        replacement = file.replaces_file(self._db)
        while replacement is not None:
            if replacement.replaces_file_uuid == old_uuid and replacement.replaces_file_date == old_date:
                return True
            replacement = replacement.replaces_file(self._db)
        return False
