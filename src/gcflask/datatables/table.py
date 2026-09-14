import datetime

import flask
from markupsafe import Markup, escape
import typing as t

from gcapp import i18n
from gcapp.i18n import tr, MLString
from gcflask.action_list import ActionList
from gcflask.datatables.queries import DataQuery
from gcapp.queries import QuerySpecification, SqlCondition, Or, Like
from gcflask.csp import csp_nonce
from medsutil import json
from nodb.interface import NODBObject


class DataColumn:

    def __init__(self,
                 name: str,
                 header_text: str,
                 allow_order: bool = False,
                 allow_search: bool = False,
                 show: bool = True,
                 formatter: t.Callable[[t.Any], t.Any] | None = None,
                 empty_value: t.Any = ''):
        self._name = name
        self._allow_order = allow_order
        self._allow_search = allow_search
        self._show = show
        self._header_text = header_text
        self._formatter = formatter
        self._empty_value = empty_value

    @property
    def name(self) -> str:
        return self._name

    @property
    def allow_order(self) -> bool:
        return self._allow_order

    @property
    def allow_search(self) -> bool:
        return self._allow_search

    @property
    def show(self) -> bool:
        return self._show

    def header(self) -> str:
        return self._header_text

    def order_by(self, is_desc: bool) -> tuple[str, bool] | None:
        return None

    def value(self, row: NODBObject) -> t.Any:
        val = self._value(row)
        if val is None:
            return self._empty_value
        elif self._formatter is not None:
            return self._formatter(val)
        else:
            return val

    def _value(self, row: NODBObject) -> t.Any:
        raise NotImplementedError

    def filter(self, value: str) -> SqlCondition | None:
        return None


class ObjectProperty(DataColumn):

    def _value(self, row):
        if hasattr(row, self.name):
            return getattr(row, self.name)
        else:
            try:
                return row[self.name]
            except (TypeError, KeyError, IndexError):
                ...
        return None

    def filter(self, value: str) -> SqlCondition | None:
        if self.allow_search:
            return Like(
                self.name,
                f"%{value}%",
                case_sensitive=False
            )
        return None

    def order_by(self, is_desc: bool) -> tuple[str, bool] | None:
        if self.allow_order:
            return self.name, is_desc
        return None


class MLStringFormatter:

    def __call__(self, value: str):
        if value is None:
            return value
        return MLString(json.load_dict(value))


class ActionListColumn(DataColumn):

    def __init__(self, action_cb: t.Callable[[t.Any], ActionList]):
        self._cb = action_cb
        super().__init__(
            name="_actions",
            header_text=i18n.tr("gcflask.action_list"),
        )

    def _value(self, row: NODBObject) -> t.Any:
        return self._cb(row).render("table_actions")


class DataTable:

    def __init__(self,
                 table_id: str,
                 query: DataQuery,
                 page_size: int | None = None,
                 ajax_route: str | None = None,
                 default_order: str | tuple[str, bool] | list[str | tuple[str, bool]] | None = None,
                 max_page_size: int = 250,
                 min_page_size: int = 10):
        self._query = query
        self._table_id = table_id
        self._page_size = page_size if page_size and page_size > 0 else None
        self._ajax_route = ajax_route
        self._columns: dict[str, DataColumn] = {}
        self._default_order = default_order
        self._allow_search = False
        self._allow_order = False
        self._min_page = min_page_size
        self._max_page = max_page_size

    def add_column(self, col: DataColumn):
        self._columns[col.name] = col
        if col.allow_order:
            self._allow_order = True
        if col.allow_search:
            self._allow_search = True

    def __str__(self) -> str:
        return str(self.build_html())

    def __html__(self) -> Markup:
        return self.build_html()

    def query_specs(self) -> QuerySpecification:
        return QuerySpecification(
            page_size=self._get_page_size(),
            page_index=self._get_page_index(),
            order_by=self._get_order_by(),
            filters=self._get_filters(),
        )

    def _get_filters(self) -> SqlCondition | None:
        conditions = []
        txt: str | None = flask.request.args.get("search[value]", default=None)
        if txt:
            for _, column in self._columns.items():
                if column.allow_search:
                    cond = column.filter(txt)
                    if cond is not None:
                        conditions.append(cond)
        if conditions:
            return Or(conditions)
        return None

    def _get_order_by(self) -> list[tuple[str, bool]] | None:
        order: list[tuple[str, bool]] = []
        if self._allow_order:
            i = 0
            while True:
                col_index = flask.request.args.get(f"order[{i}][column]")
                if col_index is None:
                    break
                col_name = str(flask.request.args.get(f"columns[{col_index}][data]"))
                order_desc = False
                if flask.request.args.get(f"order[{i}][dir]") == "desc":
                    order_desc = True
                ob = self._columns[col_name].order_by(order_desc)
                if ob is not None:
                    order.append(ob)
        return order or None

    def _get_page_index(self) -> int:
        index = flask.request.args.get("start", type=int, default=0)
        if index is None or index < 0:
            index = 0
        return index

    def _get_page_size(self) -> int | None:
        page_size = flask.request.args.get("length", type=int, default=self._page_size)
        if page_size is None or page_size < self._min_page:
            page_size = self._page_size
        elif page_size > self._max_page:
            page_size = self._max_page
        return page_size

    def build_javascript(self) -> Markup:
        config: dict[str, t.Any] = {
            "ajax": {
                "type": "POST",
            },
            "columns": [],
            "searching": False,
            "ordering": False,
            "paging": False,
            "lengthChange": False,
            "autoWidth": False,
            "language": {
                "decimal": tr("datatable.decimal"),
                "emptyTable": tr("datatable.emptyTable"),
                "info": tr("datatable.info"),
                "infoEmpty": tr("datatable.infoEmpty"),
                "infoFiltered": tr("datatable.infoFiltered"),
                "infoPostFix": "",
                "thousands": tr("datatable.thousands"),
                "lengthMenu": tr("datatable.lengthMenu"),
                "loadingRecords": tr("datatable.loadingRecords"),
                "processing": "",
                "search": tr("datatable.search"),
                "zeroRecords": tr("datatable.zeroRecords"),
                "paginate": {
                    "first": tr("datatable.paginate.first"),
                    "last": tr("datatable.paginate.last"),
                    "next": tr("datatable.paginate.next"),
                    "previous": tr("datatable.paginate.previous"),
                },
                "aria": {
                    "sortAscending": tr("datatable.aria.sortAscending"),
                    "sortDescending": tr("datatable.aria.sortDescending"),
                },
            },
        }
        if self._page_size is not None:
            config["paging"] = True
            config["pageLength"] = self._page_size
        if self._ajax_route:
            config["ajax"]["url"] = self._ajax_route
            config["serverSide"] = True
        if self._allow_search:
            config["searching"] = True
        if self._allow_order:
            config["ordering"] = True
        for cname in self._columns:
            col = self._columns[cname]
            if not col.show:
                continue
            config["columns"].append(
                {
                    "data": cname,
                    "searchable": col.allow_search,
                    "orderable": col.allow_order,
                }
            )
        block = f"<script nonce='{csp_nonce('script-src')}'>"
        block += "$(document).ready(function() {\n"
        block += "  $('#{}').DataTable({});".format(self._table_id, json.dumps(htmlsafe_json(config)))
        block += "});</script>"
        return Markup(block)

    def build_html(self) -> Markup:
        html = f'<table id="{self._table_id}" class="data_table" cellpadding="0" cellspacing="0" border="0"><thead><tr>'
        for cname in self._columns:
            if self._columns[cname].show:
                html += f'<th>{escape(self._columns[cname].header())}</th>'
        html += "</tr></thead><tbody>"
        for row in self._query.rows(self.query_specs()):
            html += "<tr>"
            for cname in self._columns:
                if self._columns[cname].show:
                    html += "<td>{}</td>".format(escape(self._columns[cname].value(row)))
            html += "</tr>"
        html += "</tbody></table>"
        return Markup(html)

    def build_ajax(self) -> dict:
        qs = self.query_specs()
        data = [
            {
                cname: escape(column.value(row))
                for cname, column in self._columns.items()
            }
            for row in self._query.rows(qs)
        ]
        return {
            "data": htmlsafe_json(data),
            "recordsTotal": self._query.count(),
            "recordsFiltered": self._query.count_filtered(qs),
            "draw": flask.request.args.get("draw", type=int)
        }

def htmlsafe_json(json):
    if isinstance(json, dict):
        for key in json:
            json[str(key)] = htmlsafe_json(json[key])
        return json
    elif isinstance(json, (list, tuple, set)):
        new_list = [htmlsafe_json(x) for x in json]
        return new_list
    elif json is None or isinstance(json, (str, bool, int, float, datetime.date)):
        return json
    elif hasattr(json, "__html__"):
        return str(json.__html__())
    elif hasattr(json, "__str__"):
        return str(json)
    else:
        raise ValueError(f"cannot serialize: {json}")