import contextvars as cv
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
        lang = CURRENT_LANGUAGE.get()
    if key in TRANSLATIONS[lang]:
        return sub_text(TRANSLATIONS[lang][key], kwargs)
    elif lang != 'und' and key in TRANSLATIONS['und']:
        return sub_text(TRANSLATIONS['und'][key], kwargs)
    elif lang != 'en' and key in TRANSLATIONS['en']:
        return sub_text(TRANSLATIONS['en'][key], kwargs)
    else:
        return f"?{key}?"


def sub_text(txt: str, subs: dict[str, str]):
    for x in subs:
        key = '{' + str(x) + '}'
        if key in txt:
            txt = txt.replace(key, subs[x])
    return txt


def set_language(lang_code: str):
    if lang_code not in ('en', 'fr', 'und'):
        raise ValueError(f'Invalid language code {lang_code}')
    CURRENT_LANGUAGE.set(lang_code)


def supported_langauges() -> dict:
    return {
        'en': 'English',
        'fr': 'Français'
    }
