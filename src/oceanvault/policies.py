import enum


class AccessType(enum.Enum):
    CREATE = "create"  # put
    READ = "read"      # get
    UPDATE = "update"  # post
    DELETE = "delete"  # delete
    LIST = "list"      # list
    ADMIN = "admin"
    DENY = "deny"


class MatchMode(enum.Enum):

    DEFAULT = 0
    REGEXP = 1


class CheckResult(enum.Enum):
    DENY = 0
    ALLOW = 1
    NO_MATCH = 2


class PolicyGroup:

    def __init__(self, policies: list[Policy]):
        self._policies = policies

    def check(self, path: str, required_access: list[AccessType]) -> CheckResult:
        return CheckResult(min(x.check(path, required_access).value for x in self._policies))


class Policy:

    def __init__(self, path: str, access: list[AccessType], mode: MatchMode):
        self._path: str = path
        self._access: list[AccessType] = access
        self._mode: MatchMode = mode

    def check(self, path: str, required_access: list[AccessType]) -> CheckResult:
        if self._matches_path(path):
            if AccessType.DENY in self._access:
                return CheckResult.DENY
            if all(x not in self._access for x in required_access):
                return CheckResult.ALLOW
            return CheckResult.DENY
        return CheckResult.NO_MATCH

    def _matches_path(self, path: str) -> bool:
        if self._mode == MatchMode.REGEXP:
            return self._regex_matches(path)
        else:
            return self._matches(path)

    def _regex_matches(self, path: str) -> bool:
        ...

    def _matches(self, path: str) -> bool:
        ...

