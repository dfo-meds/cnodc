import os
import pathlib
import sys

os.environ['CNODC_FORCE_NATIVE_JSON'] = "Y"

TEST_DIR = pathlib.Path(__file__).absolute().parent
ROOT_DIR = TEST_DIR.parent
SRC_DIR = ROOT_DIR / "src"
sys.path.append(str(SRC_DIR))
sys.path.append(str(TEST_DIR))

try:
    import logging
    import types
    import coverage
    import typing
    import zrlog
    import unittest
    from unittest import TextTestResult


    class CNODCTestResult(TextTestResult):

        SKIP_FILES = (
            'base_test_case.py',
            'Lib\\contextlib.py',
            'Lib//contextlib.py'
        )

        SKIP_ALWAYS = (
            'unittest\\case.py',
            'unittest/case.py'
        )

        def addSuccess(self, test):
            if self.dots:
                self.stream.write(".")
                self.stream.flush()
            if self.showAll:
                self.stream.write("passed\n")
                self.stream.flush()

        def addDuration(self, test, elapsed):
            super().addDuration(test, elapsed)
            if self.showAll:
                if elapsed > 0.1:
                    self.stream.write(f'\033[1;31m[{elapsed * 1000:.1f} ms]\033[0m ')
                else:
                    self.stream.write(f'[{elapsed * 1000:.1f} ms] ')
                self.stream.flush()

        def addFailure(self, test, err: tuple[type, BaseException, types.TracebackType]):
            self.failures.append((test, self._format_error(*err)))
            if self.dots:
                self.stream.write('F')
                self.stream.flush()
            elif self.showAll:
                self.stream.write(f'\033[1;31mfailed\033[0m\n')
                self.stream.flush()

        def addError(self, test, err: tuple[type, BaseException, types.TracebackType]):
            self.errors.append((test, self._format_error(*err, filter=False)))
            if self.dots:
                self.stream.write('E')
                self.stream.flush()
            elif self.showAll:
                self.stream.write(f'\033[1;31merror\033[0m\n')
                self.stream.flush()

        def _format_error(self, ex_type: type, ex: BaseException, tb: types.TracebackType, filter=True, iteration_max=10) -> str:
            traces = []
            while tb is not None:
                full_filename = tb.tb_frame.f_code.co_filename
                if full_filename.endswith(CNODCTestResult.SKIP_ALWAYS) or tb.tb_frame.f_code.co_name.startswith('assert'):
                    tb = tb.tb_next
                    continue
                elif filter and full_filename.endswith(CNODCTestResult.SKIP_FILES):
                    tb = tb.tb_next
                    continue
                else:
                    filename = full_filename
                    if filename.startswith(str(ROOT_DIR)):
                        filename = filename[len(str(ROOT_DIR))+1:]
                    line = ''
                    with open(full_filename, 'r', encoding='utf-8') as h:
                        for idx, file_line in enumerate(h.readlines(), 1):
                            if idx == tb.tb_lineno:
                                line = file_line
                                break
                    traces.append(f"{filename}:{tb.tb_lineno} [{tb.tb_frame.f_code.co_name}]: {line.strip()}")
                    tb = tb.tb_next
            s = ''
            cause = ex.__cause__
            if cause is not None:
                if iteration_max > 0:
                    s += self._format_error(type(cause), cause, cause.__traceback__, filter, iteration_max - 1)
                    s += '\nthe above was a direct cause of the following\n\n'
                else:
                    s += '\n[[traceback cutoff due to length]]\n\n'
            return s + "\n".join(reversed(traces)) + f"\n\n\033[1;31m{ex_type.__name__}\033[0m: {str(ex)}\n"



    if __name__ == '__main__':
        zrlog.init_logging()
        logging.disable(logging.NOTSET)
        logging.getLogger().setLevel('WARNING')
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter('[%(levelname)s %(asctime)s]: %(message)s [%(name)s]', '%H:%M:%S'))
        logging.getLogger().addHandler(h)
        cov = coverage.Coverage(
            config_file=TEST_DIR / ".coveragerc",
            source_pkgs=["cnodc"]
        )
        test_exact_patterns: list[tuple[str, str]] = []
        found_specific = False
        if len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                if arg == '--with-long-tests':
                    os.environ['CNODC_WITH_LONG_TESTS'] = 'Y'
                else:
                    found_specific = True
                    target = TEST_DIR / arg
                    if '.' in target.name:
                        test_exact_patterns.append((str(target.parent), target.name))
                    else:
                        test_exact_patterns.append((str(target), 'test*.py'))
        if not found_specific:
            test_exact_patterns.append((str(TEST_DIR), 'test*.py'))
        loader = unittest.TestLoader()
        cov.start()
        for target, files in test_exact_patterns:
            print(f'Searching for tests in {target} matching {files}')
            suite = loader.discover(target, files, top_level_dir=str(TEST_DIR))
            print(f'Running {len(suite._tests)} tests')
            runner = unittest.TextTestRunner(resultclass=CNODCTestResult)
            runner.run(suite)
        cov.stop()
        cov.save()
finally:
    del os.environ['CNODC_FORCE_NATIVE_JSON']
    if 'CNODC_WITH_LONG_TESTS' in os.environ:
        del os.environ['CNODC_WITH_LONG_TESTS']
