"""
    Provides a base CodedError class and some additional helper exceptions.

    CodedError is intended as a base error that includes a message, a code number, and a code space. It also
    includes our is_transient flag.

    The code space and number are intended to be unique, resulting in a string like: SPACE-CODE: MESSAGE.
    The SPACE-CODE can then be used to quickly find where the error is raised in the source code.
    The intention is to give the code space/number combination to users to report instead of stack traces.

    Transient errors are those that can be retried without any bug fixes. For example, a server being unavailable
    would raise a transient error. This supports automatic retries of code where the situation may resolve itself.
"""
import typing as t

from medsutil import ROOT_DIR
SOURCES_DIR_STR = str(ROOT_DIR)

class CodedError(Exception):
    """ An error which has a code space, code number, and is_transient flag."""

    def __init__(self, msg: str, code_number: int = None, *, code_space: str = None, is_transient: bool = False):
        if code_space is None:
            if hasattr(self, 'CODE_SPACE'):
                code_space = self.CODE_SPACE
            else:
                code_space = 'UNKNOWN'
        self._internal_code = f'{code_space}-{code_number}'
        super().__init__(f"{self._internal_code}: {msg}")
        self._is_transient = is_transient

    @property
    def internal_code(self) -> str:
        """ The internal code of the object. """
        return self._internal_code

    @property
    def is_transient(self) -> bool:
        """ Whether the error is expected to self-resolve. """
        return self._is_transient

    def obfuscated_code(self) -> str:
        """ An obfuscated version of the internal code that can be given to users. """
        return self.internal_code

    def pretty(self) -> str:
        """ A prettier representation of the error. """
        return ex_pretty(self)


class TransientCodedError(CodedError):
    """ A CodedError that is transient by default. """

    def __init__(self, msg: str, code_number: int = None, *, code_space: str = None):
        super().__init__(msg, code_number, code_space=code_space, is_transient=True)


class HaltInterrupt(KeyboardInterrupt):
    """ This interrupt extends KeyboardInterrupt and is only raised when the system is shutting down. """
    ...


def ex_pretty(e: BaseException) -> str:
    """ Formats an exception more nicely. """
    return f"{e.__class__.__name__}: {str(e)}"

def exception_kwargs_for_email(ex: BaseException) -> dict[str, str]:
    """ Retrieves additional keyword arguments to pass to a mail template for an exception. """
    return {
        'error_description': ex_pretty(ex),
        'error_details': "\n".join(format_stack_trace(ex))
    }

def format_stack_trace(ex: BaseException,
                       exclude_file_suffixes: list[str] | None = None,
                       exclude_name_prefixes: list[str] | None = None) -> list[str]:
    """ Formats a stack trace nicely into a list of strings. """
    import traceback
    efs: tuple[str] = tuple(*exclude_file_suffixes) if exclude_file_suffixes else tuple()
    enp: tuple[str] = tuple(*exclude_name_prefixes) if exclude_name_prefixes else tuple()

    lines: list[str] = []
    skip_next: bool = False
    for line in traceback.TracebackException.from_exception(ex).format(chain=True):
        add_after = False
        if line.startswith("\n"):
            lines.append("")
            add_after = True
        line = line.strip()
        if line.startswith("  File "):
            info = line.strip().split(',', maxsplit=2)
            obj_name = info[2][3:].strip()
            file_name = info[0][4:].strip(" \"")
            line_num = info[1][5:].strip()
            if obj_name.startswith(enp):
                skip_next = True
                continue
            elif file_name.endswith(efs):
                skip_next = True
                continue
            else:
                skip_next = False
                if info[0].startswith(SOURCES_DIR_STR):
                    info[0] = info[0][len(SOURCES_DIR_STR)+1:]
            lines.append(f"  File {file_name}:{line_num}, in {obj_name}")
        elif skip_next and line.startswith("    "):
            continue
        else:
            skip_next = False
            lines.append(line)
        if add_after:
            lines.append("")

    return lines


def with_exception_note[**P, Q](note: str) -> t.Callable[[t.Callable[P, Q]], t.Callable[P, Q]]:
    """ A decorator that adds a note to any exception raised. """
    import functools

    def _outer(cb: t.Callable) -> t.Callable:

        @functools.wraps(cb)
        def _inner(*args, **kwargs):
            try:
                return cb(*args, **kwargs)
            except Exception as ex:
                ex.add_note(note)
                raise
        return _inner

    return _outer
