import typing as t


class QueryManager(t.Protocol):

    def __enter__(self) -> Queryable:
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...


class Queryable(t.Protocol):

    def stream_results(self,
                       filters: SqlCondition | None = None,
                       order_by: str | tuple[str, bool] | None = None,
                       offset: int | None = None,
                       limit: int | None = None,
                       columns: list[str] | None = None):
        ...

    def count_results(self, filters: SqlCondition | None = None):
        ...


class QuerySpecification:

    def __init__(self,
                 page_index: int | None = None,
                 page_size: int | None = None,
                 order_by: list[tuple[str, bool] | str] | None = None,
                 filters: SqlCondition | None = None):
        self.page_index = page_index
        self.page_size = page_size
        self.order_by = order_by
        self.filters = filters


class SqlCondition:
    ...


class And(SqlCondition):

    def __init__(self, *conditions: SqlCondition | t.Iterable[SqlCondition]):
        self.conditions = conditions

    def all_conditions(self) -> t.Iterable[SqlCondition]:
        for condition in self.conditions:
            if isinstance(condition, SqlCondition):
                yield condition
            else:
                yield from condition


class Or(SqlCondition):

    def __init__(self, *conditions: SqlCondition | t.Iterable[SqlCondition]):
        self.conditions = conditions

    def all_conditions(self) -> t.Iterable[SqlCondition]:
        for condition in self.conditions:
            if isinstance(condition, SqlCondition):
                yield condition
            else:
                yield from condition


class Equals(SqlCondition):

    def __init__(self, column_name: str, value: t.Any, or_null: bool = False):
        self.column_name = column_name
        self.value = value
        self.or_null = or_null


class IsNull(SqlCondition):

    def __init__(self, column_name: str):
        self.column_name = column_name


class IsNotNull(SqlCondition):

    def __init__(self, column_name: str):
        self.column_name = column_name


class In(SqlCondition):

    def __init__(self, column_name: str, values: t.Iterable, or_null: bool = False):
        self.column_name = column_name
        self.values = values
        self.or_null = or_null


class Between(SqlCondition):

    def __init__(self, column_name: str, lower_bound: float, upper_bound: float, or_null: bool = False):
        self.column_name = column_name
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.or_null = or_null


class InEnvelope(SqlCondition):

    def __init__(self, column_name: str, points: list[str], datum: int = 4326, or_null: bool = False):
        self.column_name = column_name
        self.points = points
        self.datum = datum
        self.or_null = or_null


class Like(SqlCondition):

    def __init__(self, column_name: str, pattern: str, or_null: bool = False, case_sensitive: bool = True):
        self.column_name = column_name
        self.pattern = pattern
        self.or_null = or_null
        self.case_sensitive = case_sensitive
