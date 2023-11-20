import typing as t


class CNODCError(Exception):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "GEN", code_number: int = None, is_recoverable: bool = False, wrapped: t.Optional[Exception] = None):
        self.internal_code = "" if code_number is None else f"{code_space}-{code_number}"
        super().__init__(f"{msg} [{self.internal_code}]")
        self.is_recoverable = is_recoverable
        self.wrapped = wrapped

    def pretty(self) -> str:
        pass

    def obfuscated_code(self):
        return self.internal_code


class ConfigError(CNODCError):

    def __init__(self, missing_key: str, code_space: str = "GEN", code_number: int = None):
        super().__init__(f"Missing configuration key [{missing_key}]", code_space, code_number)


class CNODCHalt(CNODCError):

    def __init__(self):
        super().__init__("Application halt requested", "HALT", 1)


class CNODCNotSupported(CNODCError):

    def __init__(self, codec_name, operation):
        super().__init__(f"Codec [{codec_name}] does not support [{operation}]", "TRANSCODER", 1000, False)
