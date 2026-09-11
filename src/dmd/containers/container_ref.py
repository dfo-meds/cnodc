import typing as t

import markupsafe
import wtforms
from markupsafe import Markup
from wtforms.widgets.core import Select

import gcapp.i18n as i18n
from dmd.containers.base import ContainerLoader, Container
from dmd.containers.fields import ChoiceField, Field

from autoinject import injector, auto

from gcapp.i18n.base import BaseDString
from gcflask.forms import DynamicFormField
from gcflask.widgets import TabbedFieldFormWidget, HtmlContent, Select2Widget


class _ContainerSelectField(wtforms.Field):

    loader: ContainerLoader = auto()

    @injector.construct
    def __init__(self,
                 container_type: str,
                 allow_multiple: bool = False,
                 min_chars_to_search: int | None = None,
                 allow_select2: bool = False,
                 widget=None,
                 **kwargs):
        self._container_type = container_type
        self._allow_multiple = allow_multiple
        self._min_chars_to_search = min_chars_to_search
        self._use_select2 = False
        if widget is None:
            if allow_select2:
                widget = Select2Widget(
                    ajax_callback=self.loader.search_for_select_callback(container_type),
                    allow_multiple=allow_multiple,
                    query_delay=250,
                    placeholder=i18n.dtr("gcapp.common.placeholder"),
                    min_input=min_chars_to_search
                )
                self._use_select2 = True
            else:
                widget = Select(self._allow_multiple)
        super().__init__(**kwargs, widget=widget)

    @staticmethod
    def has_groups(): return False

    @staticmethod
    def iter_groups(): return []

    def iter_choices(self) -> t.Iterable[tuple[str, BaseDString | str, bool, dict]]:
        if not self._allow_multiple:
            yield "", i18n.dtr("gcapp.common.placeholder"), not self.data, {}
        kwargs = {}
        if self._use_select2:
            kwargs["filter_ids"] = (self.data if self._allow_multiple else [self.data]) if self.data else []
        for container_id, label in self.results(self.loader, self._container_type, **kwargs):
            if not self.data:
                is_selected = False
            else:
                is_selected = (container_id == self.data) if not self._allow_multiple else (container_id in (self.data or []))
            yield container_id, label, is_selected, {}

    @staticmethod
    def results_for_ajax(loader: ContainerLoader,
                         container_type: str,
                         name_like: str):
        return {
            "results": [
                {"id": container_id, "text": str(label)}
                for container_id, label in
                _ContainerSelectField.results(loader, container_type, name_like)
            ]
        }

    @staticmethod
    def results(loader: ContainerLoader,
                container_type: str,
                name_like: str | None = None,
                filter_ids: list[str | int] | None = None) -> t.Iterable[tuple[str, BaseDString | str]]:
        for container in loader.search_containers(
            container_type=container_type,
            filter_ids=[int(x) for x in filter_ids] if filter_ids is not None else None,
            name_like=name_like
        ):
            yield str(container.container_id), container.display()


class _InlineContainerField(DynamicFormField):
    loader: ContainerLoader = auto()

    @injector.construct
    def __init__(self,
                 container_type: str,
                 is_repeatable: bool = False,
                 allow_js_controls: bool = True,
                 original_data: dict | None = None,
                 **kwargs):
        _blank_container = self.loader.build_container(
            container_type=container_type,
            values=original_data or {}
        )
        if "widget" not in kwargs and allow_js_controls:
            kwargs["widget"] = TabbedFieldFormWidget()
        controls = {**_blank_container.controls()}
        super().__init__(controls, **kwargs)


@injector.construct
class ContainerReferenceField(Field):
    DATA_TYPE = "container_reference"
    CONTROL_CLASS = _ContainerSelectField

    loader: ContainerLoader = auto()

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        kwargs = super()._field_level_control_kwargs()
        kwargs["container_type"] = self._config.get("container_type")
        kwargs["allow_multiple"] = self.is_repeatable
        kwargs["allow_select2"] = self._allow_javascript_controls()
        min_chars = int(self._config.get("min_chars_to_search", 2))
        if min_chars > 0:
            kwargs["min_chars_to_search"] = min_chars
        return kwargs

    def _data_value(self, value: str | int | None, **kwargs) -> Container | None:
        if value is None:
            return None
        return self.loader.load_container(str(self._config.get("container_type")), int(value))

    def _display(self, v: t.Any) -> Markup:
        container = self._data_value(v)
        if container is None:
            return markupsafe.escape("")
        else:
            return container.container_link()


@injector.construct
class InlineContainerReferenceField(Field):
    DATA_TYPE = "inline_container_reference"
    CONTROL_CLASS = _InlineContainerField

    loader: ContainerLoader = auto()

    def _field_level_control_kwargs(self) -> dict[str, t.Any]:
        kwargs = super()._field_level_control_kwargs()
        kwargs.update({
            "container_type": self._config.get("container_type"),
            "original_data": self.value,
            "is_repeatable": self.is_repeatable,
            "allow_js_controls": self._allow_javascript_controls() and not self.is_repeatable
        })
        return kwargs

    def _data_value(self, values: dict[str, t.Any] | None, **kwargs) -> Container | None:
        if not values:
            return None
        return self.loader.build_container(
            container_type=str(self._config.get("container_type")),
            values=values
        )

    def _serialize(self, value: dict[str, t.Any] | None | Container) -> dict[str, t.Any] | None:
        container = self._data_value(value) if isinstance(value, dict) else value
        if container is None or container.all_empty():
            return None
        return container.serialize()

    def _display(self, value: dict[str, t.Any] | None) -> Markup | HtmlContent:
        container = self._data_value(value)
        if container is None:
            return markupsafe.escape("")
        else:
            table = container.info_table()
            table.table_classes.append("inline-container-field")
            return markupsafe.escape(table)
