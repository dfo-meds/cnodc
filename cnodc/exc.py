
class CNODCError(Exception):
    """Super-type of all errors raised by CNODC code"""

    def __init__(self, msg: str, code_space: str = "GEN", code_number: int = None, is_recoverable: bool = False):
        self.internal_code = "" if code_number is None else f"{code_space}-{code_number}"
        super().__init__(f"{msg} [{self.internal_code}]")
        self.is_recoverable = is_recoverable
