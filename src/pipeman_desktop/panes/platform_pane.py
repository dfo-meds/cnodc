from medsutil import json
from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.components.inputs import FormDialog, StringField, CheckboxField, SelectField, LengthValidator, IntegerField, DateTimeField, FloatField, \
    RangeValidator, Required
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.scrollable import ScrollableTreeview
import gcapp.i18n.base as i18n
import tkinter as tk
import typing as t
import tkinter.ttk as ttk
from autoinject import injector

if t.TYPE_CHECKING:
    from pipeman_desktop.main_app import PipemanDesktop



class PlatformDialog(FormDialog):

    PLATFORM_IDS = ("wmo_id", "wigos_id", "platform_name", "platform_id")

    PLATFORM_STATUS: dict[str, str] = {
        "ACTIVE": "platform_status.active",
        "HISTORICAL": "platform_status.historical",
        "REMOVED": "platform_status.removed",
        "REPLACED": "paltform_status.replaced",
    }

    PLATFORM_TYPES: dict[str, str] = {
        "glider": "platform.glider",
    }

    SPEED_UNITS: list[str] = ['m s-1', 'knots']

    def __init__(self, parent, defaults = None, readonly = False):
        super().__init__(parent, i18n.tr("dialog.platform.title"), defaults, readonly)

    def fields(self):
        self.add_field("platform_type", SelectField(
            options=self.PLATFORM_TYPES,
            label_name="dialog.platform.platform_type",
            tooltip_name="tooltip.platform.platform_type",
            validators=[Required()]
        ))
        self.add_field("status", SelectField(
            options=self.PLATFORM_STATUS,
            label_name="dialog.platform.status",
            tooltip_name="tooltip.platform.status",
            validators=[Required()]
        ))
        self.add_field("wmo_id", IntegerField(
            label_name="dialog.platform.wmo_id",
            tooltip_name="tooltip.platform.wmo_id",
            validators=[LengthValidator(5, 7)]
        ))
        self.add_field("wigos_id", StringField(
            label_name="dialog.platform.wigos_id",
            tooltip_name="tooltip.platform.wigos_id",
            validators=[LengthValidator(max_length=126)]
        ))
        self.add_field("platform_id", StringField(
            label_name="dialog.platform.platform_id",
            tooltip_name="tooltip.platform.platform_id",
            validators=[LengthValidator(max_length=126)]
        ))
        self.add_field("platform_name", StringField(
            label_name="dialog.platform.platform_name",
            tooltip_name="tooltip.platform.platform_name",
            validators=[LengthValidator(max_length=126)]
        ))
        self.add_field("start_date", DateTimeField(
            label_name="dialog.platform.start_date",
            tooltip_name="tooltip.platform.start_date",
        ))
        self.add_field("end_date", DateTimeField(
            label_name="dialog.platform.start_date",
            tooltip_name="tooltip.platform.start_date",
        ))
        self.add_field("embargo_data_days", IntegerField(
            label_name="dialog.platform.embargo_data_days",
            tooltip_name="tooltip.platform.embargo_data_days",
            validators=[RangeValidator(min_value=0)]
        ))
        self.add_field("dedupe_time_window", FloatField(
            label_name="dialog.platform.dedupe_time_window",
            tooltip_name="tooltip.platform.dedupe_time_window",
            validators=[RangeValidator(min_value=0)]
        ))
        self.add_field("dedupe_distance_window", FloatField(
            label_name="dialog.platform.dedupe_distance_window",
            tooltip_name="tooltip.platform.dedupe_distance_window",
            validators=[RangeValidator(min_value=0)]
        ))
        self.add_field("map_to_uuid", StringField(
            label_name="dialog.platform.map_to_uuid",
            tooltip_name="tooltip.platform.map_to_uuid",
        ))
        self.add_field("top_speed", FloatField(
            label_name="dialog.platform.top_speed",
            tooltip_name="tooltip.platform.top_speed",
            control_colspan=1,
            validators=[RangeValidator(min_value=0)]
        ))
        self.add_field("top_speed_units", SelectField(
            options=self.SPEED_UNITS,
            same_row_as="top_speed",
            no_label=True,
            col_offset=2
        ))
        self.add_field("skip_speed_check", CheckboxField(
            label_name="dialog.platform.skip_speed_check",
            tooltip_name="tooltip.platform.skip_speed_check",
            col_offset=1
        ))
        self.add_field("skip_land_check", CheckboxField(
            label_name="dialog.platform.skip_land_check",
            tooltip_name="tooltip.platform.skip_land_check",
            col_offset=1
        ))
        self.add_field("mandatory_review", CheckboxField(
            label_name="dialog.platform.mandatory_review",
            tooltip_name="tooltip.platform.mandatory_review",
            col_offset=1
        ))

    def custom_validation(self) -> list[str] | None:
        errors = []
        if all(not self.value(x) for x in self.PLATFORM_IDS):
            errors.append(i18n.tr("errors.platform.no_id_provided"))
        return errors

    def alter_data(self, data: dict):
        if data["top_speed"] and data["top_speed_units"]:
            data["top_speed"] = f"{data["top_speed"]} {data["top_speed_units"]}"
        del data["top_speed_units"]


class PlatformPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._platform_list: t.Optional[ScrollableTreeview] = None
        self._can_update_platform: dict[str, bool] = {}
        self._pane_id: str | None = None

    def on_init(self):
        self.app.menus.add_command("qc/reload_platforms", "menu.reload_platforms", self._reload_platforms, start_disabled=True)
        self.app.menus.add_command("qc/create_platform", "menu.create_platform", self._create_platform, start_disabled=True)
        station_frame = ttk.Frame(self.app.batch_bottom)
        station_frame.rowconfigure(0, weight=1)
        station_frame.columnconfigure(0, weight=1)
        self._platform_list = ScrollableTreeview(
            parent=station_frame,
            selectmode="browse",
            show="headings",
            columns=["uuid", "wmo_id", "wigos_id", "name", "id", "start", "end"],
            on_right_click=self._on_right_click
        )
        self._platform_list.set_header_text("uuid", i18n.tr("tree.platform_list.uuid"))
        self._platform_list.set_header_text("wmo_id", i18n.tr("tree.platform_list.wmo_id"))
        self._platform_list.set_header_text("wigos_id", i18n.tr("tree.platform_list.wigos_id"))
        self._platform_list.set_header_text("name", i18n.tr("tree.platform_list.name"))
        self._platform_list.set_header_text("id", i18n.tr("tree.platform_list.id"))
        self._platform_list.set_header_text("start", i18n.tr("tree.platform_list.start"))
        self._platform_list.set_header_text("end", i18n.tr("tree.platform_list.end"))
        self._platform_list.grid(row=0, column=0, sticky='NSEW')
        self.app.batch_bottom.add(station_frame, text=i18n.tr('pane.platform_list'), sticky='NSEW')
        self._pane_id = self.app.batch_bottom.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.USER | DisplayChange.SAVING):
            self.app.menus.set_state('qc/reload_platforms', app_state.has_access('desktop.find_platform'))
            self.app.menus.set_state('qc/create_platform', app_state.can_save_platform())
        if change_type & (DisplayChange.BATCH_STATE | DisplayChange.PLATFORMS):
            self._update_platform_list()
        if change_type & DisplayChange.LANGUAGE:
            if self._platform_list is not None:
                self._platform_list.set_header_text("uuid", i18n.tr("tree.platform_list.uuid"))
                self._platform_list.set_header_text("wmo_id", i18n.tr("tree.platform_list.wmo_id"))
                self._platform_list.set_header_text("wigos_id", i18n.tr("tree.platform_list.wigos_id"))
                self._platform_list.set_header_text("name", i18n.tr("tree.platform_list.name"))
                self._platform_list.set_header_text("id", i18n.tr("tree.platform_list.id"))
                self._platform_list.set_header_text("start", i18n.tr("tree.platform_list.start"))
                self._platform_list.set_header_text("end", i18n.tr("tree.platform_list.end"))
            if self._pane_id is not None:
                self.app.batch_bottom.tab(self._pane_id, text=i18n.tr("pane.platform_list"))

    def _update_platform_list(self):
        self._platform_list.clear_items()
        self._can_update_platform.clear()
        with self.local_db.cursor() as cur:
            cur.execute("""
                SELECT platform_uuid, wmo_id, wigos_id, platform_name, platform_id, service_start_date, service_end_date, actions FROM platforms
            """)
            for row in cur.fetchall():
                self._platform_list.append_item(
                    iid=row[0],
                    parent='',
                    values=tuple(x or "" for x in row[:-1]),
                    text=''
                )
                self._can_update_platform[row[0]] = "update" in json.load_dict(row[-1])

    def _create_platform(self):
        s = PlatformDialog(self.app.root)
        if s.result is not None:
            self.app.state.update_save_flags(saving_platform=True)
            self.app.dispatcher.submit_job(
                'pipeman_desktop.client.api_client.create_platform',
                job_kwargs=s.result,
                on_success=self._on_platform_creation,
                on_error=self._on_platform_creation_failure
            )

    def _on_platform_creation(self, res: str | None):
        self.app.state.update_save_flags(saving_platform=False)
        if res:
            self.app.state.refresh_display(DisplayChange.PLATFORMS)

    def _on_platform_creation_failure(self, ex):
        self.app.show_user_exception(ex)
        self.app.state.update_save_flags(saving_platform=False)

    def _reload_platforms(self):
        self.app.menus.disable_command('qc/reload_platforms')
        self.app.dispatcher.submit_job(
            'pipeman_desktop.client.api_client.reload_platforms',
            on_success=self._reload_success,
            on_error=self._reload_error
        )

    def _reload_success(self, res):
        self.app.menus.enable_command('qc/reload_platforms')
        self.app.state.refresh_display(DisplayChange.PLATFORMS)

    def _reload_error(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('qc/reload_platforms')
        self.app.state.refresh_display(DisplayChange.PLATFORMS)

    def _on_right_click(self, item, event):
        cm = PlatformContextMenu(
            self.app,
            item["iid"],
            self._can_update_platform[item["iid"]] if item["iid"] in self._can_update_platform else False,
        )
        cm.handle_popup_click(event)


class PlatformContextMenu:

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self,
                 app: PipemanDesktop,
                 platform_uuid: str,
                 can_update: bool):
        self._menu = tk.Menu(app.root, tearoff=0)
        self._app = app
        self._platform_uuid = platform_uuid
        self._menu.add_command(
            label=i18n.tr("context_menu.platform.view"),
            command=self._view_platform
        )
        if can_update:
            self._menu.add_command(
                label=i18n.tr("context_menu.platform.update"),
                command=self._update_platform,
            )
        if self._app.state.current_parent is not None:
            self._menu.add_command(
                label=i18n.tr("context_menu.platform.assign_to_record"),
                command=self._assign_to_record
            )
        if self._app.state.batch_state is not None:
            self._menu.add_command(
                label=i18n.tr("context_menu.platform.assign_to_all"),
                command=self._assign_to_all
            )

    def _platform_info(self) -> dict:
        with self.local_db.cursor() as cur:
            cur.execute("SELECT wmo_id, wigos_id, platform_name, platform_id, platform_type, service_start_date, service_end_date, metadata, map_to_uuid, status, embargo_data_days FROM platforms WHERE platform_uuid = ?", (self._platform_uuid,))
            row = cur.fetchone()
            if row:
                metadata = json.load_dict(row[7]) if row[7] else {}
                return {
                    "wmo_id": row[0],
                    "wigos_id": row[1],
                    "platform_name": row[2],
                    "platform_id": row[3],
                    "platform_type": row[4],
                    "service_start_date": row[5],
                    "service_end_date": row[6],
                    "map_to_uuid": row[8],
                    "status": row[9],
                    "embargo_data_days": row[10],
                    "skip_speed_check": metadata.get("skip_speed_check", False),
                    "skip_land_check": metadata.get("skip_on_land_check", False),
                    "mandatory_review": metadata.get("mandatory_review", False),
                    "top_speed": metadata.get("top_speed", None),
                    "dedupe_time_window": metadata.get("dedupe_time_window", None),
                    "dedupe_distance_window": metadata.get("dedupe_distance_window", None),
                }
        return {}

    def _view_platform(self):
        s = PlatformDialog(self._app.root, defaults=self._platform_info(), readonly=True)

    def _assign_to_record(self):
        self._app.state.update_record_platform(self._app.state.current_working_uuid, self._platform_uuid)

    def _assign_to_all(self):
        for idx, record in self._app.state.batch_records.items():
            self._app.state.update_record_platform(record.record_uuid, self._platform_uuid)

    def _update_platform(self):
        s = PlatformDialog(self._app.root, self._platform_info())
        if s.result is not None:
            self._app.state.update_save_flags(saving_platform=True)
            self._app.dispatcher.submit_job(
                'pipeman_desktop.client.api_client.update_platform',
                job_kwargs=s.result,
                on_success=self._on_platform_update,
                on_error=self._on_platform_update_failure
            )

    def _on_platform_update(self, res):
        self._app.state.update_save_flags(saving_platform=False)
        if res:
            self._app.state.refresh_display(DisplayChange.PLATFORMS)

    def _on_platform_update_failure(self, ex: Exception):
        self._app.show_user_exception(ex)
        self._app.state.update_save_flags(saving_platform=False)

    def handle_popup_click(self, e):
        try:
            self._menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            self._menu.grab_release()
