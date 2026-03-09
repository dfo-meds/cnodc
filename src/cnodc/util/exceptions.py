import typing as t


class CNODCError(Exception):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "GEN", code_number: int = None, is_recoverable: bool = False, wrapped: t.Optional[Exception] = None):
        self.internal_code = "" if code_number is None else f"{code_space}-{code_number}"
        super().__init__(f"{self.internal_code}: {msg}")
        self._msg = msg
        self.is_recoverable = is_recoverable
        self.wrapped = wrapped

    def obfuscated_code(self):
        return self.internal_code


class ConfigError(CNODCError):

    def __init__(self, missing_key: str, code_number: int = None):
        super().__init__(f"Missing configuration key [{missing_key}]", "CONFIG", code_number)


class NotSupportedError(Exception):
    pass


class HaltInterrupt(KeyboardInterrupt):
    pass  # pragma: no cover


class DynamicObjectLoadError(CNODCError):
    pass
