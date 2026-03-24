import typing as t


class CNODCError(Exception):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "GEN", code_number: int = None, is_transient: bool = False):
        self.internal_code = f'{code_space}-{code_number}'
        super().__init__(f"{self.internal_code}: {msg}")
        self.is_transient = is_transient

    def obfuscated_code(self):
        return self.internal_code


class TransientError(CNODCError):

    def __init__(self, msg: str, code_space: str = 'GEN', code_number: int = None):
        super().__init__(msg, code_space, code_number, is_transient=True)


class ConfigError(CNODCError):

    def __init__(self, missing_key: str, code_number: int = None):
        super().__init__(f"Missing configuration key [{missing_key}]", "CONFIG", code_number)


class NotSupportedError(Exception):
    pass


class HaltInterrupt(KeyboardInterrupt):
    pass  # pragma: no cover


class DynamicObjectLoadError(CNODCError):
    pass
