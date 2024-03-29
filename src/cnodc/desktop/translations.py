import contextvars as cv
import typing as t


TRANSLATIONS = {
    'und': {
        'choice_dialog_ok': 'Selectionnez | Select',
        'language_select_dialog_title': 'Langage | Language',
    },
    'en': {
        'choice_dialog_ok': 'Select',
        'language_select_dialog_title': 'Language',
        'root_title': 'CNODC - QC Dashboard',
        'menu_file': 'File',
        'menu_login': 'Login',
        'no_user_logged_in': '(none)',
        'user_logged_in': '{username} ',
        'error_message_title': 'Error',
        'login_dialog_title': 'Login',
        'login_dialog_username': 'Username',
        'login_dialog_password': 'Password',
        'invalid_credentials': 'Invalid username and/or password',
        'remote_api_error': 'An error occurred on the server: {message}\nError code: {code}',
        'station_reload': 'Refresh Station List',
        'menu_qc': 'Quality Control',
        'menu_reload_stations': 'Sync Stations',
        'parameter_list_name': 'Element',
        'parameter_list_value': 'Value',
        'parameter_list_units': 'Units',
        'parameter_list_quality': 'Q',
        'parameter_context_edit': 'Edit',
        'parameter_context_flag_good': 'Good (1)',
        'parameter_context_flag_probably_good': 'Probably Good (2)',
        'parameter_context_flag_dubious': 'Dubious (3)',
        'parameter_context_flag_erroneous': 'Erroneous (4)',
        'parameter_context_flag_missing': 'Missing (9)',
        'prompt_min_value': 'Minimum: {min}',
        'prompt_max_value': 'Maximum: {max}',
        'prompt_range': 'Range: [{min}, {max}]',
        'hour': 'Hour',
        'minute': 'Minute',
        'second': 'Second',
        'timezone': 'TZ',
        'metadata': 'Metadata',
        'coordinates': 'Coordinates',
        'parameters': 'Parameters',
        'data_type_choice_title': 'Choose Data Type',
        'data_type_choice_prompt': 'Select the type of data',
        'data_type_string': 'Text',
        'data_type_integer': 'Integer',
        'data_type_datetime': 'Date and Time',
        'data_type_date': 'Date',
        'data_type_decimal': 'Real Number',
        'data_type_not_supported_title': 'Not Supported',
        'data_type_not_supported_message': 'Editing {data_type} elements is not supported',
        'close_without_saving_title': 'Unsaved Changes',
        'close_without_saving_message': 'You have unsaved changes, are you sure you want to close the batch?',
        'menu_next_station_failure': 'Load Next Station Failure',
        'action_item_name': 'Action',
        'action_item_object': 'Target',
        'action_item_value': 'New Value',
        'remove': 'Remove',
        'record_set_label_profile': 'Profile #{index}',
        'history_time': 'Time',
        'history_message': 'Message',
        'history_source': 'Origin',
        'history_type': 'Type',
        'message_type_i': 'Informational',
        'message_type_n': 'Note',
        'message_type_w': 'Warning',
        'message_type_e': 'Error',
        'flag_at_and_below': 'Flag [{flag}] at position and below',
        'flag_at_and_right': 'Flag [{flag}] at position and right',
        'qc_test_name': 'Test Name',
        'qc_test_time': 'Time Run',
        'qc_test_error_name': 'Error',
        'qc_test_element_name': 'Element',
        "record_list_title": "Records",
        "child_record_list_title": "Child Records",
        "record_label": "Record {index}",
        "qc_test_nodb_station_check": "Station Check",
        "goto": "Goto",
        "tooltip_descalate": "De-escalate",
        "tooltip_escalate": "Escalate",
        "tooltip_fail": "Report Error",
        "tooltip_release": "Release",
        "tooltip_complete": "Submit",
        "tooltip_load_next": "Submit and Load Next (Ctrl-N)",
        "tooltip_save": "Save (Ctrl-S)",
        "tooltip_load_new": "Load",
        "station_list": "Stations",
        "menu_change_password": "Change Password",
        "menu_logout": "Logout",
        "menu_create_station": "Create Station",
        "password_change_1": "Password",
        "password_change_2": "Repeat Password",
        'password_change_message': 'Your new password must be at least 15 characters long.',
        'password_change_success_title': 'Password Changed',
        'password_change_success_message': 'Your password has been changed',
        'logout_success_title': 'Logged Out',
        'logout_success_message': 'You have been logged out.',
        'qc_error_station_no_id': 'No Station ID Detected',
        'qc_error_station_no_record': 'No Station Match Found',
        'qc_error_station_bad_uuid': 'Invalid CNODC Station ID',
        'qc_error_station_incomplete': 'Incomplete Station Record',
        'qc_error_station_many_records': 'Several Station Matches Found',
    },
    'fr': {
        'choice_dialog_ok': 'Selectionnez',
        'language_select_dialog_title': 'Langage',
        'root_title': 'CNDOC - Tableau de bord pour CQ',
    }
}


CURRENT_LANGUAGE = cv.ContextVar[str]("current_language", default="und")


def get_text(key: str, lang: str = None, **kwargs: str):
    if lang is None:
        lang = current_language()
    if key in TRANSLATIONS[lang]:
        return sub_text(TRANSLATIONS[lang][key], kwargs)
    elif lang != 'und' and key in TRANSLATIONS['und']:
        return sub_text(TRANSLATIONS['und'][key], kwargs)
    elif lang != 'en' and key in TRANSLATIONS['en']:
        return sub_text(TRANSLATIONS['en'][key], kwargs)
    else:
        return f"?{key}?"


def get_text_from_dict(options: dict[str, str], lang: str = None, default: t.Optional[str] = None):
    if lang is None:
        lang = current_language()
    if lang in options:
        return options[lang]
    elif lang != 'und' and 'und' in options:
        return options['und']
    elif lang != 'en' and 'en' in options:
        return options['en']
    else:
        return default or str(options)


def sub_text(txt: str, subs: dict[str, str]):
    for x in subs:
        key = '{' + str(x) + '}'
        if key in txt:
            txt = txt.replace(key, subs[x])
    return txt


def current_language() -> str:
    return CURRENT_LANGUAGE.get()


def set_language(lang_code: str):
    if lang_code not in ('en', 'fr', 'und'):
        raise ValueError(f'Invalid language code {lang_code}')
    CURRENT_LANGUAGE.set(lang_code)


def supported_langauges() -> dict:
    return {
        'en': 'English',
        'fr': 'Français'
    }
