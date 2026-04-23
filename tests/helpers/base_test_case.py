import datetime
import logging
import pathlib
import shutil
import sys
import tempfile
import threading
import typing
import unittest
import unittest as ut
from contextlib import contextmanager
from unittest import mock

import zrlog
from autoinject import injector
from medsutil.awaretime import AwareDateTime
from medsutil.halts import DummyHaltFlag
from nodb import NODB
from tests.helpers.mock_containers import TestContainer, NODBContainer
from tests.helpers.mock_runner import WorkerTestController
from medsutil.exceptions import CodedError
from tests.helpers.mock_nodb import DatabaseMock, DummyNODB
from tests.helpers.mock_requests import QuickWebMock
import typing as t

@injector.injectable
class InjectableDict:

    def __init__(self):
        self.data = {}

TEST_FILE_DIR = pathlib.Path(__file__).absolute().resolve().parent.parent / 'test_data'

SKIP_FLAG = threading.Event()

def skip_long_test(test_case):
    if SKIP_FLAG.is_set():
        return unittest.skip('skipping long tests')(test_case)
    return test_case

def ordered_test[**P, Q](x: int) -> t.Callable[P,Q]:
    def _inner(tc: t.Callable[P,Q]) -> t.Callable[P,Q]:
        tc._meds_test_order_ = x
        return tc
    return _inner

def ordered_after[**P, Q](x: t.Callable) -> t.Callable[P,Q]:
    def _inner(tc: t.Callable[P,Q]) -> t.Callable[P,Q]:
        if not hasattr(x, '_meds_test_order_'):
            x._meds_test_order_ = 0
        tc._meds_test_order_ = x._meds_test_order_ + 1
        return tc
    return _inner


def load_ordered_tests(loader: ut.TestLoader, tests: list[ut.TestSuite | ut.TestCase], pattern: str):
    work: list[ut.TestCase | ut.TestSuite] = []
    work.extend(tests)
    cases: list[ut.TestCase] = []
    def _sort_test_objects(x: ut.TestCase | ut.TestSuite):
        if isinstance(x, ut.TestSuite):
            work.extend(x._tests)
        else:
            cases.append(x)
    while work:
        _sort_test_objects(work.pop())
    suite = ut.TestSuite()
    def _sort_by_test_order(x: ut.TestCase) -> int:
        test_function = getattr(x, x._testMethodName)
        if hasattr(test_function, '_meds_test_order_'):
            return test_function._meds_test_order_
        return sys.maxsize
    suite.addTests(sorted(cases, key=_sort_by_test_order))
    return suite

class ClassProperty[RetType]:

    def __init__(self, fget: typing.Callable[..., RetType]):
        self._fget = fget

    def __get__(self, instance, cls) -> RetType:
        return self._fget.__get__(instance, cls)()

    def __call__(self, fn):
        return ClassProperty(fn)

def classproperty[RetType](fn: typing.Callable[..., RetType]) -> ClassProperty[RetType]:
    return ClassProperty[RetType](fn)


class BaseTestCase(ut.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addTypeEqualityFunc(datetime.datetime, self.assertSameTime)
        self.addTypeEqualityFunc(AwareDateTime, self.assertSameTime)
        self.addTypeEqualityFunc(dict, self.assertDictSimilar)
        self._log = zrlog.get_logger(f'test.{self.__class__.__qualname__}')

    @staticmethod
    def data_file_path(rel_path: str = None) -> pathlib.Path:
        if rel_path is None:
            return TEST_FILE_DIR
        return TEST_FILE_DIR / rel_path

    @contextmanager
    def temp_data_file(self, rel_path: str, metadata: bool = False):
        file_path = BaseTestCase.data_file_path(rel_path)
        md_file_path = None if not metadata else (file_path.parent / f"{file_path.name}.metadata")
        try:
            yield file_path if md_file_path is None else (file_path, md_file_path)
        finally:
            file_path.unlink(True)
            if md_file_path is not None:
                md_file_path.unlink(True)

    @contextmanager
    def temp_data_dir(self, rel_path: str, metadata: bool = False):
        dir_path = BaseTestCase.data_file_path(rel_path)
        md_file_path = None if not metadata else (dir_path.parent / f"{dir_path.name}.metadata")
        try:
            yield dir_path if md_file_path is None else (dir_path, md_file_path)
        finally:
            if dir_path.exists():
                shutil.rmtree(dir_path)
            if md_file_path is not None:
                md_file_path.unlink(True)

    @classproperty
    @classmethod
    def real_nodb(cls) -> NODB:
        if not hasattr(cls, '_real_nodb'):
            cls._real_nodb = NODBContainer()
            cls.enterClassContext(cls._real_nodb)
        return cls._real_nodb.nodb

    @classmethod
    def set_log_level_for_class(cls, new_level):
        if not hasattr(cls, '_old_log_level'):
            cls._old_log_level = logging.getLogger().level or logging.NOTSET
        logging.getLogger().setLevel(new_level)
        cls.addClassCleanup(cls._clean_log_level)

    @classmethod
    def _clean_log_level(cls):
        if hasattr(cls, '_old_log_level'):
            logging.getLogger().setLevel(cls._old_log_level)

    @classmethod
    def setUpClass(cls):
        cls.reset_db_between_tests: bool = True

    @classproperty
    @classmethod
    def worker_controller(cls):
        if not hasattr(cls, '_worker_controller'):
            cls._worker_controller = WorkerTestController(cls.db, cls.mock_nodb, cls.halt_flag)
        return cls._worker_controller

    @classproperty
    @classmethod
    def halt_flag(cls):
        if not hasattr(cls, '_halt_flag'):
            cls._halt_flag = DummyHaltFlag()
        return cls._halt_flag

    @classproperty
    @classmethod
    def class_temp_dir(cls):
        if not hasattr(cls, '_class_temp_dir'):
            cls._class_temp_dir = pathlib.Path(tempfile.mkdtemp()).resolve().absolute()
            cls.addClassCleanup(cls._clean_up_class_temp)
        return cls._class_temp_dir

    @classmethod
    def _clean_up_class_temp(cls):
        if hasattr(cls, '_class_temp_dir'):
            shutil.rmtree(cls._class_temp_dir)
            del cls._class_temp_dir

    @classproperty
    @classmethod
    def web(cls):
        if not hasattr(cls, '_web'):
            cls._web = QuickWebMock()
        return cls._web

    @classproperty
    @classmethod
    def mock_nodb(cls):
        if not hasattr(cls, '_mock_nodb'):
            cls._mock_nodb = DummyNODB(cls.db)
        return cls._mock_nodb

    @classproperty
    @classmethod
    def db(cls):
        if not hasattr(cls, '_db'):
            cls._db = DatabaseMock()
        return cls._db

    def tearDown(self):
        self._clean_injectable()
        self._clean_db()
        self._clean_halt_flag()

    def _clean_halt_flag(self):
        if hasattr(self, '_halt_flag'):
            self._halt_flag.event.clear()

    def _clean_db(self):
        if hasattr(self, 'reset_db_between_tests') and self.reset_db_between_tests and hasattr(self, '_db'):
            self._db.reset()

    @injector.inject
    def _clean_injectable(self, d: InjectableDict = None):
        d.data.clear()

    @classmethod
    def start_container_by_name(cls, name, always_restart: bool = False) -> TestContainer:
        return cls.start_container(TestContainer(name, always_restart))

    @classmethod
    def start_container(cls, container: TestContainer) -> TestContainer:
        cls.enterClassContext(container)
        return container

    @property
    def temp_dir(self):
        if not hasattr(self, '_temp_dir'):
            self._temp_dir = pathlib.Path(tempfile.mkdtemp()).resolve().absolute()
            self.addCleanup(self._clean_up_temp_dir)
        return self._temp_dir

    def _clean_up_temp_dir(self):
        if hasattr(self, '_temp_dir'):
            shutil.rmtree(self._temp_dir)
            del self._temp_dir

    @classmethod
    @contextmanager
    def mock_web_test(cls):
        with mock.patch('requests.get', side_effect=cls.web.mock_get):
            with mock.patch('requests.post', side_effect=cls.web.mock_post):
                with mock.patch('requests.request', side_effect=cls.web.mock_request) as x:
                    yield x

    @contextmanager
    def assertRaisesCNODCError(self, error_code: str = None, is_transient: bool = None, msg=None):
        with self.assertRaises(CodedError) as h:
            yield h
        if (error_code and error_code != h.exception.internal_code) or (is_transient is not None and is_transient is not h.exception.is_transient):
            self._log.warning(msg or f"'{error_code}[{'any' if is_transient is None else is_transient}]' != '{h.exception.internal_code}[{h.exception.is_transient}]'")

    @contextmanager
    def assertNoError(self, msg: str = None):
        try:
            with self.assertRaises(Exception) as h:
                yield h
        except AssertionError:
            return True
        except Exception as ex:
            raise self.failureException(msg or f"Found unexpected error: {ex.__class__.__name__}: {ex}") from ex

    @contextmanager
    def assertLogs(self, logger=None, level=None):
        old_level = logging.root.disabled
        try:
            if level:
                if isinstance(level, str):
                    logging.disable(getattr(logging, level) - 1)
                else:
                    logging.disable(level - 1)
            else:
                logging.disable(logging.NOTSET)
            with super().assertLogs(logger, level) as h:
                yield h
        finally:
            logging.disable(old_level)

    def assertSameTime(self, dt1: datetime.datetime | str | None, dt2: datetime.datetime | str, msg=None):
        if isinstance(dt1, str):
            dt1 = datetime.datetime.fromisoformat(dt1)
        if isinstance(dt2, str):
            dt2 = datetime.datetime.fromisoformat(dt2)
        msg = msg or f'[{dt1.isoformat() if dt1 else None}] != [{dt2.isoformat() if dt2 else None}]'
        if dt1 is None and dt2 is not None:
            raise self.failureException(msg + " (dt1 is none")
        elif dt2 is None and dt1 is not None:
            raise self.failureException(msg + " (dt2 is none")
        elif dt1 is None and dt2 is None:
            return
        elif dt1.tzinfo and not dt2.tzinfo:
            raise self.failureException(msg + " (cannot compare aware to naive datetimes")
        elif dt2.tzinfo and not dt1.tzinfo:
            raise self.failureException(msg + " (cannot compare naive to aware datetimes")
        elif dt1.tzinfo is not None and dt2.tzinfo is not None:
            dt2 = dt2.astimezone(dt1.tzinfo)
        self.assertEqual((dt1 - dt2).total_seconds(), 0, msg=msg)

    def assertDictSimilar(self, d1, d2, msg = None, strict_order: bool = False):
        errors = self._compare_dicts(d1, d2, strict_order=strict_order)
        if errors:
            msg = msg or 'Dictionaries are not equal'
            msg += '\n- ' + '\n- '.join(errors)
            msg += f'\n ({len(errors)} errors found)'
            raise self.failureException(msg)

    def assertListEqualNoOrder(self, l1, l2, msg = None, strict_order: bool = False):
        errors = self._compare_lists(l1, l2, strict_order=strict_order)
        if errors:
            msg = msg or 'Lists are not equal'
            msg += '\n' + '\n'.join(errors)
            raise self.failureException(msg)

    def _compare_item(self, i1, i2, prefix='', strict_order: bool = False) -> list[str]:
        errors = []
        if isinstance(i1, dict):
            if not isinstance(i2, dict):
                errors.append(f'Item {prefix} is a dict in first, but not in second')
            else:
                errors.extend(self._compare_dicts(i1, i2, prefix, strict_order=strict_order))
        elif isinstance(i1, (list, set, tuple)):
            if not isinstance(i1, (list, set, tuple)):
                errors.append(f'Item {prefix} is a list in the first, but not in second')
            else:
                errors.extend(self._compare_lists(i1, i2, prefix, strict_order=strict_order))
        elif i1 != i2:
            errors.append(f'first{prefix} ({i1}) != second[{prefix} ({i2})')
        return errors

    def _compare_lists(self, l1, l2, prefix='', strict_order: bool = False) -> list[str]:
        errors = []
        if strict_order:
            for idx in range(0, max(len(l1), len(l2))):
                if idx >= len(l1):
                    errors.append(f'first missing {prefix}[{idx}]')
                elif idx >= len(l2):
                    errors.append(f'second missing {prefix}[{idx}]')
                else:
                    errors.extend(self._compare_item(l1[idx], l2[idx], prefix + f'[{idx}]'))
        else:
            l2_indexes_seen = set()
            for item in l1:
                for idx, item2 in enumerate(l2):
                    if idx in l2_indexes_seen:
                        continue
                    if item == item2:
                        l2_indexes_seen.add(idx)
                        break
                else:
                    errors.append(f'item {item} missing in second{prefix}')
            for idx, item2 in enumerate(l2):
                if idx not in l2_indexes_seen:
                    errors.append(f'item {item2} missing in first{prefix}')
        return errors

    def _compare_dicts(self, d1, d2, prefix='', strict_order: bool = False) -> list[str]:
        keys1 = list(d1.keys())
        keys2 = list(d2.keys())
        errors = []
        for key in keys1:
            if key not in keys2:
                errors.append(f"Key {prefix}[{key}] found in first, but not in second")
        for key in keys2:
            if key not in keys1:
                errors.append(f"Key {prefix}[{key}] found in second, but not in first")
            else:
                errors.extend(self._compare_item(d1[key], d2[key], prefix + f'[{key}]', strict_order=strict_order))
        return errors
