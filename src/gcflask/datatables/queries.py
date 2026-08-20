import typing as t
from nodb.interface import NODB, SqlCondition, And


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


class DataQuery:

    nodb: NODB

    def __init__(self,
                 object_cls,
                 filters: SqlCondition | None = None,
                 limit_fields: list[str] | None = None):
        self._obj_cls = object_cls
        self._base_filters = filters or {}
        self._limit_fields = limit_fields or []

    def rows(self, query_spec: QuerySpecification):
        with self.nodb as db:
            for result in db.stream_objects(self._obj_cls, **self._rows_kwargs(query_spec)):
                yield result

    def count(self):
        with self.nodb as db:
            return db.count_objects(self._obj_cls, **self._base_kwargs())

    def count_filtered(self, query_spec: QuerySpecification):
        with self.nodb as db:
            return db.count_objects(self._obj_cls, **self._filtered_kwargs(query_spec))

    def _rows_kwargs(self, query_spec: QuerySpecification) -> dict[str, t.Any]:
        base = self._filtered_kwargs(query_spec)
        base['limit_fields'] = self._limit_fields
        if query_spec.page_size is not None:
            base['offset'] = (query_spec.page_index or 0) * query_spec.page_size
            base['limit'] = query_spec.page_size
        if query_spec.filters is not None:
            base['filters'].update(query_spec.filters)
        if query_spec.order_by is not None:
            base['order_by'] = query_spec.order_by
        return base

    def _filtered_kwargs(self, query_spec: QuerySpecification) -> dict[str, t.Any]:
        base = self._base_kwargs()
        if query_spec.filters is not None:
            if base["filters"] is None:
                base["filters"] = query_spec.filters
            else:
                base["filters"] = And(base["filters"], query_spec.filters)
        return base

    def _base_kwargs(self) -> dict[str, t.Any]:
        return {
            'filters': self._base_filters
        }





