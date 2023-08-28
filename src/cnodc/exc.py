
class CNODCError(Exception):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "GEN", code_number: int = None, is_recoverable: bool = False):
        self.internal_code = "" if code_number is None else f"{code_space}-{code_number}"
        super().__init__(f"{msg} [{self.internal_code}]")
        self.is_recoverable = is_recoverable

    def pretty(self) -> str:
        pass


class ConfigError(CNODCError):

    def __init__(self, missing_key: str, code_space: str = "GEN", code_number: int = None):
        super().__init__(f"Missing configuration key [{missing_key}]", code_space, code_number)