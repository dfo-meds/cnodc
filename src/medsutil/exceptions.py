

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
