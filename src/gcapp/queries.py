""" Support for a generic querying framework against a database. """
import typing as t


class QueryManager[X](t.Protocol):
    """ Represents a contextmanager that yields a Queryable context object. """

    def __enter__(self) -> Queryable[X]:
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...


class Queryable[X](t.Protocol):
    """ An object that can stream and count results from a SQL query."""

    def stream_results(self,
                       filters: SqlCondition | None = None,
                       order_by: str | tuple[str, bool] | None = None,
                       offset: int | None = None,
                       limit: int | None = None,
                       columns: list[str] | None = None) -> t.Iterable[X]:
        ...

    def count_results(self, filters: SqlCondition | None = None) -> int:
        ...


class QuerySpecification:
    """ Contains filters, orders, and limit information for a SQL query."""

    def __init__(self,
                 page_index: int | None = None,
                 page_size: int | None = None,
                 order_by: list[tuple[str, bool] | str] | None = None,
                 filters: SqlCondition | None = None):
        """
        :param page_index: The index of the current page (default is None or 0)
        :param page_size: The size of the current page (default is None for all the results)
        :param order_by: A list of column names (or tuples of column names and booleans which are True to indicate a descending order).
        :param filters: A SqlCondition object to filter by
        """
        self.page_index = page_index
        self.page_size = page_size
        self.order_by = order_by
        self.filters = filters


class SqlCondition:
    """ Represents a SQL condition that can be translated into SQL code. """
    ...


class And(SqlCondition):
    """ SQL "AND" statement - all given conditions must be true. """

    def __init__(self, *conditions: SqlCondition | t.Iterable[SqlCondition]):
        self.conditions = conditions

    def all_conditions(self) -> t.Iterable[SqlCondition]:
        for condition in self.conditions:
            if isinstance(condition, SqlCondition):
                yield condition
            else:
                yield from condition


class Or(SqlCondition):
    """ SQL "OR" statement - at least one of the given conditions must be true. """

    def __init__(self, *conditions: SqlCondition | t.Iterable[SqlCondition]):
        self.conditions = conditions

    def all_conditions(self) -> t.Iterable[SqlCondition]:
        for condition in self.conditions:
            if isinstance(condition, SqlCondition):
                yield condition
            else:
                yield from condition


class Equals(SqlCondition):
    """ The value of a column must be equal to the given literal value. """

    def __init__(self, column_name: str, value: t.Any, or_null: bool = False, case_sensitive: bool = True):
        """

            :param column_name: The name of the column
            :param value: The literal value to check
            :param or_null: Allow NULL matches as well (equivalent to Or(Equals, IsNull))
            :param case_sensitive: If value is a string, setting this to False will perform a case-insensitive comparison.
        """
        self.column_name = column_name
        self.value = value
        self.or_null = or_null
        self.case_sensitive = case_sensitive or not isinstance(self.value, str)


class IsNull(SqlCondition):
    """ The value of the column must be null. """

    def __init__(self, column_name: str):
        """
            :param column_name: The name of the column that must be null.
        """
        self.column_name = column_name


class IsNotNull(SqlCondition):
    """ The value of the column must not be null. """

    def __init__(self, column_name: str):
        """
            :param column_name: The name of the column that must not be null.
        """
        self.column_name = column_name


class In(SqlCondition):
    """ The value of the column must be in a given set of values. """

    def __init__(self, column_name: str, values: t.Collection, or_null: bool = False):
        """

        :param column_name: The column name to check
        :param values: The values to check against
        :param or_null: Allow NULL matches as well.
        """
        self.column_name = column_name
        self.values = values
        self.or_null = or_null


class Between(SqlCondition):
    """ The value of the column must be between a given lower and upper bound. """

    def __init__(self, column_name: str, lower_bound: float, upper_bound: float, or_null: bool = False):
        """
            :param column_name: The column name to check
            :param lower_bound: The lower bound
            :param upper_bound: The upper bound
            :param or_null: Allow NULL matches as well.
        """
        self.column_name = column_name
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.or_null = or_null


class InEnvelope(SqlCondition):
    """ Where supported, a geodesy comparison to ensure the given column is within a polygon defined by the points. """

    def __init__(self, column_name: str, points: list[str], datum: int = 4326, or_null: bool = False):
        """
        :param column_name: The column name to check
        :param points: A list of points (WKT formatting)
        :param datum: The datum to use
        :param or_null: Whether to accept NULL matches as well.
        """
        self.column_name = column_name
        self.points = points
        self.datum = datum
        self.or_null = or_null


class Like(SqlCondition):
    """ A SQL LIKE comparison """

    def __init__(self, column_name: str, pattern: str, or_null: bool = False, case_sensitive: bool = True):
        """
        :param column_name: The column name to check
        :param pattern: The LIKE pattern to ma
        :param or_null: Whether to allow NULL matches as well.
        :param case_sensitive: If true, uses ILIKE instead
        """
        self.column_name = column_name
        self.pattern = pattern
        self.or_null = or_null
        self.case_sensitive = case_sensitive
