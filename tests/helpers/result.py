import pathlib
import types
from unittest import TextTestResult, TextTestRunner

ROOT_DIR = pathlib.Path(__file__).absolute().parent.parent.parent


class CNODCRunner(TextTestRunner):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, resultclass=CNODCTestResult)


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

    def _format_error(self, ex_type: type, ex: BaseException, tb: types.TracebackType | None, filter=True, iteration_max=10) -> str:
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
                try:
                    with open(full_filename, 'r', encoding='utf-8') as h:
                        for idx, file_line in enumerate(h.readlines(), 1):
                            if idx == tb.tb_lineno:
                                line = file_line
                                break
                except OSError:
                    pass
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
        return s + "\n".join(traces) + f"\n\n\033[1;31m{ex_type.__name__}\033[0m: {str(ex)}\n"
