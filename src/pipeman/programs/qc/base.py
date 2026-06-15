import abc
import collections.abc
import contextlib
import functools
import typing as t

import zrlog
from pip_audit._dependency_source import requirement

import medsutil.math as amath
from medsutil import ocproc2 as ocproc2
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.math import _functions
from medsutil.ocproc2.refs import ElementType, AnyRef, ElementRef, SingleElementRef, MultiElementRef, \
    RecordSetRef, RecordRef, ParentRecordRef, ChildRecordRef, RecordCrawler
from medsutil.ocproc2.util import QualityError, CoordinateTracker, check_quality, RequiredQuality, ObjectWithMetadata, \
    Quality
from medsutil.ocproc_math import extract_parameter_value
from medsutil.units import UnitConverter
from autoinject import injector

from pipeman.programs.qc.reference_ranges import ReferenceRange

if t.TYPE_CHECKING:
    class QCMethodProtocol[R: AnyRef](t.Protocol):
        def __call__(_, self: QualityController, ref: R, *args, **kwargs) -> t.Any:
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
                 test_tags: list[str] | None = None):
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

    def setup(self): ...

    def extract_parameter_value(self,
                                parameter_name: str,
                                ref: SingleElementRef,
                                units: str | None = None,
                                required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE,
                                obs_date: AwareDateTime | None = None) -> amath.AnyNumber | None:
        return extract_parameter_value(parameter_name, ref.element, units, required_quality, obs_date if obs_date is not None else self.current_time)

    def run_record_check(self, record: ocproc2.ParentRecord) -> ocproc2.QCTestRunInfo:
        self._current_record = ParentRecordRef(record=record, path="", parent=None)
        self.setup()
        try:
            self.run()
            if self._reviews_passed == 0 and self._reviews_failed == 0:
                self.update_qc_result(ocproc2.QCResult.SKIP)
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
            test_tags=list(self._test_tags),
            test_date=AwareDateTime.utcnow(),
            result=self._qc_result,
            messages=self._qc_messages,
            notes=f"passed={self._reviews_passed};failed={self._reviews_failed};skipped={self._reviews_skipped}",
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
                    self.set_working_quality(pass_flag, ref.ref_object)
        except QCSkipReview as ex:
            self._log.info("review %s skipped: %s on [%s]", review_name, ex, refs)
            self._reviews_skipped += 1
        except QCAssertionError as ex:
            self._log.info("review %s failed: %s on [%s]", review_name, ex, refs)
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

    def check_quality(self, element: ObjectWithMetadata | None, required_quality: RequiredQuality):
        try:
            check_quality(element, required_quality)
        except QualityError as ex:
            self.skip_review(str(ex))

    def check_review_already_complete(self,
                                      references: t.List[AnyRef] | AnyRef | ObjectWithMetadata,
                                      required_quality: RequiredQuality = RequiredQuality.QC_INCOMPLETE):
        if isinstance(references, list):
            any_passed: bool = False
            any_skipped: bool = False
            for ref in references:
                try:
                    self.check_review_already_complete(ref.ref_object)
                    any_passed = True
                except QCSkipReview:
                    any_skipped = True
            if any_skipped and not any_passed:
                self.skip_review("all_completed")
        elif isinstance(references, AnyRef):
            self.check_quality(references.ref_object, required_quality=required_quality)
        else:
            self.check_quality(references, required_quality=required_quality)

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

    def set_working_quality(self,
                            working_quality: int,
                            element: ocproc2.AbstractElement | ocproc2.RecordSet | ocproc2.BaseRecord):
        existing_quality = element.metadata.best("WorkingQuality", default=None, coerce=int)
        if Quality.new_quality_allowed(working_quality, existing_quality):
            element.metadata["WorkingQuality"] = working_quality

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

    def extract_good_values(self,
                            element: SingleElementRef | MultiElementRef | None,
                            required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE) -> t.Iterable[SingleElementRef]:
        if element is not None:
            for element_sref in element.single_element_refs():
                try:
                    self.require_quality(element_sref.element, required_quality=required_quality)
                    yield element_sref
                except QCSkipReview:
                    ...

    def require_quality(self,
                        value: ocproc2.AbstractElement | None,
                        required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE) -> t.TypeGuard[ocproc2.AbstractElement]:
        self.check_quality(value, required_quality)
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
        if not _functions.between(min_value, a, max_value):
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
        if not amath.gte(a, b):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_greater_or_close", **kwargs)

    def assert_less_or_close(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not amath.lte(a, b):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_greater(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not amath.gt(a, b):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_greater_or_close", **kwargs)

    def assert_less(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not amath.lt(a, b):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_close(self, a: amath.AnyNumber, b: amath.AnyNumber, msg: str | None = None, **kwargs):
        if not amath.is_close(a, b):
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_less_or_close", **kwargs)

    def assert_not_equal(self, a: t.Any, b: t.Any, msg: str | None = None, **kwargs):
        if a == b:
            if not kwargs.get("ref_value", None):
                kwargs["ref_value"] = str(b)
            self.report_qc_error(msg or "not_equal", **kwargs)


class DeepDiveChecker(QualityController):

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
        crawler = RecordCrawler(
            element_cb=self._wrap_callback(self.element_check),
            single_element_cb=self._wrap_callback(self.single_element_check),
            multi_element_cb=self._wrap_callback(self.multi_element_check),
            parent_record_cb=self._wrap_callback(self.parent_record_check),
            child_record_cb=self._wrap_callback(self.child_record_check),
            record_cb=self._wrap_record_callback(self.record_check),
            recordset_cb=self._wrap_callback(self.recordset_check),
            limit_element_types=self.LIMIT_ELEMENT_TYPES,
            limit_subrecord_types=self.LIMIT_SUBRECORD_TYPES
        )
        crawler.crawl_record(self.current_record)

    def _wrap_record_callback(self, cb):
        if not self.TRACK_COORDINATES:
            return self._wrap_callback(cb)
        elif cb is not None:
            @functools.wraps(cb)
            def _inner(ref, *args, **kwargs):
                self._update_coordinates(ref)
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
        self.profile_check(profile)
        self._profile_memory = None

    def profile_check(self, profile: list[ChildRecordRef]):
        for ref in profile:
            self._update_coordinates(ref)
            self.level_check(ref)

    def level_check(self, ref: ChildRecordRef):
        ...
