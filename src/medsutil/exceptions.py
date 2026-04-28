import traceback
import pathlib

SOURCES_DIR = pathlib.Path(__file__).absolute().resolve().parent.parent
SOURCES_DIR_STR = str(SOURCES_DIR)

class CodedError(Exception):

    def __init__(self, msg: str, code_number: int = None, *, code_space: str = None, is_transient: bool = False):
        if code_space is None:
            if hasattr(self, 'CODE_SPACE'):
                code_space = self.CODE_SPACE
            else:
                code_space = 'UNKNOWN'
        self.internal_code = f'{code_space}-{code_number}'
        super().__init__(f"{self.internal_code}: {msg}")
        self.is_transient = is_transient

    def obfuscated_code(self) -> str:
        return self.internal_code

    def pretty(self) -> str:
        return ex_pretty(self)


class TransientCodedError(CodedError):

    def __init__(self, msg: str, code_number: int = None, *, code_space: str = None):
        super().__init__(msg, code_number, code_space=code_space, is_transient=True)


class HaltInterrupt(KeyboardInterrupt): ...


def ex_pretty(e: BaseException):
    return f"{e.__class__.__name__}: {str(e)}"

def exception_kwargs_for_email(ex: BaseException) -> dict[str, str]:
    return {
        'error_description': ex_pretty(ex),
        'error_details': "\n".join(format_stack_trace(ex, colorize=False))
    }

def format_stack_trace(ex: BaseException,
                       exclude_file_suffixes: list[str] | None = None,
                       exclude_name_prefixes: list[str] | None = None,
                       colorize: bool = True) -> list[str]:
    efs: tuple[str] = tuple(*exclude_file_suffixes) if exclude_file_suffixes else tuple()
    enp: tuple[str] = tuple(*exclude_name_prefixes) if exclude_name_prefixes else tuple()

    lines: list[str] = []
    skip_next: bool = False
    for line in traceback.TracebackException.from_exception(ex).format(chain=True, colorize=colorize):
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


def color_text(text: str, color: int, style: int = 0) -> str:
    return f"\033[{style};{color}m{text}\033[0m"