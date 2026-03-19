import datetime
import logging
import pathlib
import shutil
import tempfile
import unittest as ut
from unittest.result import TestResult
import zoneinfo
from contextlib import contextmanager
from unittest import mock, TextTestResult

from autoinject import injector

from cnodc.util.awaretime import AwareDateTime
from cnodc.util.halts import DummyHaltFlag
from helpers.mock_runner import WorkerTestController
from cnodc.util import CNODCError
from helpers.db_mock import DatabaseMock, DummyNODB
from helpers.web_mock import QuickWebMock


@injector.injectable
class InjectableDict:

    def __init__(self):
        self.data = {}


class BaseTestCase(ut.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addTypeEqualityFunc(datetime.datetime, self.assertSameTime)
        self.addTypeEqualityFunc(AwareDateTime, self.assertSameTime)
        self.addTypeEqualityFunc(dict, self.assertDictSimilar)

    @classmethod
    def setUpClass(cls):
        cls.class_temp_dir = pathlib.Path(tempfile.mkdtemp()).resolve().absolute()
        cls.db = DatabaseMock()
        cls.nodb = DummyNODB(cls.db)
        cls.web = QuickWebMock()
        cls.halt_flag = DummyHaltFlag()
        cls.worker_controller = WorkerTestController(cls.db, cls.nodb, cls.halt_flag)

    def setUp(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp()).resolve().absolute()

    @injector.inject
    def tearDown(self, d: InjectableDict = None):
        shutil.rmtree(self.temp_dir)
        self.db.reset()
        d.data.clear()
        self.halt_flag.event.clear()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.class_temp_dir)

    @contextmanager
    def mock_web_test(self):
        with mock.patch('requests.get', side_effect=self.web.mock_get):
            with mock.patch('requests.post', side_effect=self.web.mock_post):
                with mock.patch('requests.request', side_effect=self.web.mock_request) as x:
                    yield x

    @contextmanager
    def assertRaisesCNODCError(self, error_code: str):
        with self.assertRaises(CNODCError) as h:
            yield h
        self.assertEqual(error_code, h.exception.internal_code)

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

    def assertSameTime(self, dt1: datetime.datetime, dt2: datetime.datetime, msg=None):
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
