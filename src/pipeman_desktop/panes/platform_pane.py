from medsutil import json
from medsutil.awaretime import AwareDateTime
from nodb.observations import PlatformStatus
from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.components.bordered_entry import BorderedEntry, BorderedControl, BorderedChoice, BorderedCheckbox
from pipeman_desktop.components.tooltip import Tooltip
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.scrollable import ScrollableTreeview
import gcapp.i18n.base as i18n
import tkinter.simpledialog as tksd
import tkinter as tk
import typing as t
import tkinter.ttk as ttk
from autoinject import injector



class PlatformDialog(tksd.Dialog):

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

    def __init__(self, parent, station_info: dict[str, t.Any] | None = None) -> None:
        defaults = station_info or {}
        self._platform_display_options: list[str] = []
        self._platform_actual_options: list[str] = []
        for actual, display in self.PLATFORM_TYPES.items():
            self._platform_display_options.append(i18n.tr(display))
            self._platform_actual_options.append(actual)
        self._platform_status_display_options: list[str] = []
        self._platform_status_actual_options: list[str] = []
        for actual, display in self.PLATFORM_STATUS.items():
            self._platform_status_display_options.append(i18n.tr(display))
            self._platform_status_actual_options.append(actual)
        self.wmo_id = tk.StringVar(parent, value=defaults.get("wmo_id", "") or "")
        self.wigos_id = tk.StringVar(parent, value=defaults.get("wigos_id", "") or "")
        self.platform_id = tk.StringVar(parent, value=defaults.get("platform_id", "") or "")
        self.platform_name = tk.StringVar(parent, value=defaults.get("platform_name", "") or "")
        platform_actual_type = defaults.get("platform_type", "") or ""
        platform_display = ""
        if platform_actual_type and platform_actual_type in self._platform_actual_options:
            platform_display = self._platform_display_options[self._platform_actual_options.index(platform_actual_type)]
        self.platform_type = tk.StringVar(parent, value=platform_display)
        self.embargo_days = tk.StringVar(parent, value=defaults.get("embargo_data_days", "") or "")
        self.map_to_uuid = tk.StringVar(parent, value=defaults.get("map_to_uuid", "") or "")
        self.skip_speed_check = tk.IntVar(parent, value=defaults.get("skip_speed_check", 0))
        self.skip_land_check = tk.IntVar(parent, value=defaults.get("skip_land_check", 0))
        self.dedupe_time_window = tk.StringVar(parent, value=defaults.get("dedupe_time_window", "") or "")
        self.dedupe_distance_window = tk.StringVar(parent, value=defaults.get("dedupe_distance_window", "") or "")
        top_speed = ""
        top_speed_units = "m s-1"
        if "top_speed" in defaults and defaults["top_speed"] and " " in defaults["top_speed"]:
            top_speed, top_speed_units = defaults["top_speed"].split(" ")
        self.top_speed = tk.StringVar(parent, value=top_speed)
        self.top_speed_units = tk.StringVar(parent, value=top_speed_units)
        status = defaults.get("status", "ACTIVE")
        status_display = ""
        if status and status in self._platform_status_actual_options:
            status_display = self._platform_status_display_options[self._platform_status_actual_options.index(status)]
        self.status = tk.StringVar(parent, value=status_display)
        self.start_date = tk.StringVar(parent, value=defaults.get("service_start_date", "") or "")
        self.end_date = tk.StringVar(parent, value=defaults.get("service_end_date", "") or "")
        self.result: dict | None = None
        self._controls: dict[str, tuple[BorderedControl | ttk.Entry | ttk.Checkbutton | ttk.Combobox, ttk.Label | None, Tooltip | None]] = {}
        super().__init__(parent=parent, title=i18n.tr('dialog.platform.title'))

    def _add_text_control(self, parent, label_text: str, textvar: tk.StringVar, row: int, column: int = 0, colspan: int = 2):
        label = ttk.Label(parent, text=i18n.tr(f"dialog.platform.{label_text}"))
        label.grid(row=row, column=column, sticky="W", padx=2, pady=2)
        entry = BorderedEntry(parent, textvariable=textvar)
        entry.grid(row=row, column=column + 1, sticky="EW", columnspan=colspan, padx=2, pady=2)
        tooltip = Tooltip(label, f"tooltip.platform.{label_text}")
        self._controls[label_text] = (entry, label, tooltip)

    def _add_checkbox_control(self, parent, label_text: str, intvar: tk.IntVar, row: int, column: int = 1, colspan: int = 2):
        button = BorderedCheckbox(parent, text=i18n.tr(f"dialog.platform.{label_text}"), onvalue=1, offvalue=0, variable=intvar)
        button.grid(row=row, column=column, columnspan=colspan, padx=2, pady=2, sticky="W")
        tooltip = Tooltip(button, f"tooltip.platform.{label_text}")
        self._controls[label_text] = (button, None, tooltip)

    def _add_choice_control(self, parent, label_text: str, values: list[str], textvar: tk.StringVar, row: int, column: int = 0, colspan: int = 2):
        label = ttk.Label(parent, text=i18n.tr(f"dialog.platform.{label_text}"))
        label.grid(row=row, column=column, sticky="W", padx=2, pady=2)
        entry = BorderedChoice(parent, values=values, textvariable=textvar)
        entry.grid(row=row, column=column + 1, sticky="EW", columnspan=colspan, padx=2, pady=2)
        tooltip = Tooltip(label, f"tooltip.platform.{label_text}")
        self._controls[label_text] = (entry, label, tooltip)

    def body(self, parent):
        self._add_text_control(parent, "wmo_id", self.wmo_id, 0)
        self._add_text_control(parent, "wigos_id", self.wigos_id, 1)
        self._add_text_control(parent, "platform_id", self.platform_id, 2)
        self._add_text_control(parent, "platform_name", self.platform_name, 3)
        self._add_choice_control(parent, "platform_type", self._platform_display_options, self.platform_type, 4)
        self._add_choice_control(parent, "status", self._platform_status_display_options, self.status, 5)
        self._add_text_control(parent, "start_date", self.start_date, 6)
        self._add_text_control(parent, "end_date", self.end_date, 7)
        self._add_text_control(parent, "embargo_days", self.embargo_days, 8)
        self._add_text_control(parent, "dedupe_time_window", self.dedupe_time_window, 9)
        self._add_text_control(parent, "dedupe_distance_window", self.dedupe_distance_window, 10)
        self._add_text_control(parent, "map_to_uuid", self.map_to_uuid, 11)
        self._add_text_control(parent, "top_speed", self.top_speed, 12, colspan=1)
        units_box = ttk.Combobox(parent, values=self.SPEED_UNITS, textvariable=self.top_speed_units)
        units_box.grid(row=12, column=2, sticky='EW', padx=2, pady=2)
        self._controls["top_speed_units"] = (units_box, None, None)
        self._add_checkbox_control(parent, "skip_speed_check", self.skip_speed_check, 13)
        self._add_checkbox_control(parent, "skip_land_check", self.skip_land_check, 14)

    def _validate_str_coerce(self,
                             x: str,
                             coerce: t.Callable[[str], t.Any] = str,
                             allow_empty: bool = True,
                             min_value: float | None = None,
                             max_value: float | None = None,
                             max_length: int | None = None) -> bool:
        if allow_empty and x == "":
            return True
        try:
            value = coerce(x)
            if max_length and len(value) > max_length:
                return False
            if min_value is not None and value < min_value:
                return False
            if max_value is not None and value > max_value:
                return False
            return True
        except (ValueError, TypeError, IndexError):
            return False

    def set_control_error(self, control_name, is_error: bool):
        if control_name in self._controls:
            control = self._controls[control_name]
            if isinstance(control, BorderedControl):
                control.set_errored(is_error)

    def validate(self):
        check = True

        if not self._validate_str_coerce(self.embargo_days.get(), int, min_value=0):
            check = False
            self.set_control_error("embargo_days", True)
        else:
            self.set_control_error("embargo_days", False)

        if not self._validate_str_coerce(self.dedupe_distance_window.get(), float, min_value=0):
            check = False
            self.set_control_error("dedupe_distance_window", True)
        else:
            self.set_control_error("dedupe_distance_window", False)

        if not self._validate_str_coerce(self.dedupe_time_window.get(), float, min_value=0):
            check = False
            self.set_control_error("dedupe_time_window", True)
        else:
            self.set_control_error("dedupe_time_window", False)

        if not self._validate_str_coerce(self.top_speed.get(), float, min_value=0):
            check = False
            self.set_control_error("top_speed", True)
        else:
            self.set_control_error("top_speed", False)

        wmo_id = self.wmo_id.get()
        if not self._validate_str_coerce(wmo_id, int, min_value=0):
            check = False
            self.set_control_error("wmo_id", True)
        elif len(wmo_id) not in (5, 7):
            check = False
            self.set_control_error("wmo_id", True)
        else:
            self.set_control_error("wmo_id", False)

        if not self._validate_str_coerce(self.start_date.get(), AwareDateTime.fromisoformat):
            check = False
            self.set_control_error("start_date", True)
        else:
            self.set_control_error("start_date", False)

        if not self._validate_str_coerce(self.end_date.get(), AwareDateTime.fromisoformat):
            check = False
            self.set_control_error("end_date", True)
        else:
            self.set_control_error("end_date", False)

        wigos_id = self.wigos_id.get()
        if not self._validate_str_coerce(wigos_id, max_length=126):
            check = False
            self.set_control_error("wigos_id", True)
        else:
            self.set_control_error("wigos_id", False)

        platform_name = self.platform_name.get()
        if not self._validate_str_coerce(platform_name, max_length=126):
            check = False
            self.set_control_error("platform_name", True)
        else:
            self.set_control_error("platform_name", False)

        platform_id = self.platform_id.get()
        if not self._validate_str_coerce(platform_id, max_length=126):
            check = False
            self.set_control_error("platform_id", True)
        else:
            self.set_control_error("platform_id", False)

        if all(x == '' for x in (wmo_id, wigos_id, platform_name, platform_id)):
            check = False
            self.set_control_error("platform_name", True)
            self.set_control_error("platform_id", True)
            self.set_control_error("wigos_id", True)
            self.set_control_error("wmo_id", True)

        return check

    def apply(self):
        if self.validate():
            result: dict[str, t.Any] = {
                "wmo_id": self.wmo_id.get() or None,
                "wigos_id": self.wigos_id.get() or None,
                "platform_id": self.platform_id.get() or None,
                "platform_name": self.platform_name.get() or None,
                "map_to_uuid": self.map_to_uuid.get() or None,
                "skip_speed_check": self.skip_speed_check.get() == 1,
                "skip_land_check": self.skip_land_check.get() == 1,
                "platform_type": self._platform_actual_options[self._platform_display_options.index(self.platform_type.get())],
                "status": PlatformStatus(self._platform_status_actual_options[self._platform_status_display_options.index(self.status.get())]),
            }
            sd = self.start_date.get()
            result["start_date"] = AwareDateTime.fromisoformat(sd) if sd else None
            ed = self.end_date.get()
            result["end_date"] = AwareDateTime.fromisoformat(ed) if ed else None
            embargo_days = self.embargo_days.get()
            result["embargo_data_days"] = int(embargo_days) if embargo_days else None
            dd_time = self.dedupe_time_window.get()
            result["dedupe_time_window"] = float(dd_time) if dd_time else None
            dd_distance = self.dedupe_distance_window.get()
            result["dedupe_distance_window"] = float(dd_distance) if dd_distance else None
            top_speed = self.top_speed.get()
            if top_speed:
                top_speed_units = self.top_speed_units.get()
                result["top_speed"] = f"{float(top_speed)} {top_speed_units}"
            else:
                result["top_speed"] = None
            self.result = result



class PlatformPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._platform_list: t.Optional[ScrollableTreeview] = None
        self._can_update_platform: dict[str, bool] = {}
        self._pane_id: str | None = None

    def on_init(self):
        self.app.menus.add_sub_menu('qc', 'menu.qc')
        self.app.menus.add_command("qc/reload_platforms", "menu.reload_platforms", self._reload_platforms, start_disabled=True)
        self.app.menus.add_command("qc/create_platform", "menu.create_platform", self._create_platform, start_disabled=True)
        station_frame = ttk.Frame(self.app.middle_bottom)
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
        self.app.middle_bottom.add(station_frame, text=i18n.tr('pane.platform_list'), sticky='NSEW')
        self._pane_id = self.app.middle_bottom.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.USER | DisplayChange.SAVING):
            self.app.menus.set_state('qc/reload_platforms', app_state.has_access('desktop.find_platform'))
            self.app.menus.set_state('qc/create_platform', app_state.can_save_platform())
        elif change_type & DisplayChange.BATCH_STATE:
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
                self.app.middle_bottom.tab(self._pane_id, text=i18n.tr("pane.platform_list"))

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
        if res:
            self._update_platform_list()
        self.app.state.update_save_flags(saving_platform=False)

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
        self._update_platform_list()
        self.app.menus.enable_command('qc/reload_platforms')

    def _reload_error(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('qc/reload_platforms')

    def _on_right_click(self, item, *args):
        # TODO: menu options for editing and assigning to the current record, if appropriate
        pass
