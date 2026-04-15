import medsutil.exceptions as exceptions

class CNODCError(exceptions.CodedError):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "CNODC", code_number: int = None, is_transient: bool = False):
        super().__init__(msg, code_number, code_space=code_space, is_transient=is_transient)

    def obfuscated_code(self):
        return self.internal_code


class TransientError(CNODCError):

    def __init__(self, msg: str, code_space: str = 'CNODC', code_number: int = None):
        super().__init__(msg, code_space, code_number, is_transient=True)


class ConfigError(CNODCError):

    def __init__(self, missing_key: str, code_number: int = None):
        super().__init__(f"Missing configuration key [{missing_key}]", "CONFIG", code_number)


class NotSupportedError(Exception):
    pass

