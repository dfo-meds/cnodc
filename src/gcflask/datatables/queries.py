import typing as t

from gcapp.queries import QuerySpecification, QueryManager, SqlCondition, And


class DataQuery:

    def __init__(self,
                 queryable: QueryManager,
                 filters: SqlCondition | None = None,
                 limit_fields: list[str] | None = None):
        self._manager = queryable
        self._base_filters = filters or None
        self._limit_fields = limit_fields or []

    def rows(self, query_spec: QuerySpecification):
        with self._manager as db:
            for result in db.stream_results(**self._rows_kwargs(query_spec)):
                yield result

    def count(self):
        with self._manager as db:
            return db.count_results(**self._base_kwargs())

    def count_filtered(self, query_spec: QuerySpecification):
        with self._manager as db:
            return db.count_results(**self._filtered_kwargs(query_spec))

    def _rows_kwargs(self, query_spec: QuerySpecification) -> dict[str, t.Any]:
        base = self._filtered_kwargs(query_spec)
        base['columns'] = self._limit_fields
        if query_spec.page_size is not None:
            base['offset'] = (query_spec.page_index or 0) * query_spec.page_size
            base['limit'] = query_spec.page_size
        if query_spec.filters is not None:
            if base.get("filters"):
                base['filters'] = And(base["filters"], query_spec.filters)
            else:
                base["filters"] = query_spec.filters
        if query_spec.order_by is not None:
            base['order_by'] = query_spec.order_by
        return base

    def _filtered_kwargs(self, query_spec: QuerySpecification) -> dict[str, t.Any]:
        base = self._base_kwargs()
        if query_spec.filters is not None:
            if base.get("filters"):
                base["filters"] = And(base["filters"], query_spec.filters)
            else:
                base["filters"] = query_spec.filters
        return base

    def _base_kwargs(self) -> dict[str, t.Any]:
        return {
            'filters': self._base_filters
        }





