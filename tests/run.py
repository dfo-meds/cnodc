import pathlib
import types
import unittest
from unittest import TextTestResult

import typing as t
import coverage
import sys

TEST_DIR = pathlib.Path(__file__).absolute().parent
ROOT_DIR = TEST_DIR.parent
SRC_DIR = ROOT_DIR / "src"


sys.path.append(str(SRC_DIR))
sys.path.append(str(TEST_DIR))


class CNODCTestResult(TextTestResult):

    def addFailure(self, test, err: tuple[type, BaseException, types.TracebackType]):
        self.failures.append((test, self._format_error(*err)))
        if self.dots:
            self.stream.write('F')

    def addError(self, test, err: tuple[type, BaseException, types.TracebackType]):
        self.errors.append((test, self._format_error(*err)))
        if self.dots:
            self.stream.write('E')

    def _format_error(self, ex_type: type, ex: BaseException, tb: types.TracebackType) -> str:
        s = ''
        while tb is not None:
            full_filename = tb.tb_frame.f_code.co_filename
            if (not full_filename.endswith(('unittest\\case.py', 'unittest/case.py', 'base_test_case.py'))) and not tb.tb_frame.f_code.co_name.startswith('assert'):
                filename = full_filename
                if filename.startswith(str(ROOT_DIR)):
                    filename = filename[len(str(ROOT_DIR))+1:]
                line = ''
                with open(full_filename, 'r', encoding='utf-8') as h:
                    for idx, file_line in enumerate(h.readlines(), 1):
                        if idx == tb.tb_lineno:
                            line = file_line
                            break
                s += f"{filename}:{tb.tb_lineno} [{tb.tb_frame.f_code.co_name}]: {line.strip()}\n"
            tb = tb.tb_next
        s += f"\n\033[1;31m{ex_type.__name__}\033[0m: {str(ex)}\n"
        return s


if __name__ == '__main__':
    cov = coverage.Coverage(
        config_file=TEST_DIR / ".coveragerc",
        source_pkgs=["cnodc"]
    )
    target = str(TEST_DIR)
    files = 'test*.py'
    if len(sys.argv) > 1:
        target = TEST_DIR / sys.argv[1]
        if target.name.endswith('.py'):
            files = target.name
            target = target.parent
    loader = unittest.TestLoader()
    cov.start()
    suite = loader.discover(target, files)
    runner = unittest.TextTestRunner(resultclass=CNODCTestResult)
    runner.run(suite)
    cov.stop()
    cov.save()


