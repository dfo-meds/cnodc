import typing as t

from gcapp.i18n import MLString


class Keyword:

    def __init__(self,
                 identifier: str,
                 machine_key: str | None = None,
                 translated_values: str | None = None,
                 thesaurus: dict | None = None,
                 mode: str = "value"):
        self._identifier = identifier
        self._machine_key = machine_key
        self._translations = translated_values or {}
        self._thesaurus = thesaurus or {}
        self._mode = mode

    @property
    def use_machine_key(self) -> bool:
        return self._machine_key is not None and self._machine_key != "" and self._mode == "both"

    @property
    def use_value_only(self) -> bool:
        return self._machine_key is not None and self._machine_key != "" and self._mode == "value"

    def __str__(self):
        if self._machine_key:
            return str(MLString({"und": self._machine_key, **self._translations}))
        else:
            return str(MLString(self._translations))

    def to_display(self, primary_locale, use_prefixes: bool = False, prefix_separator: str = ":", force_translations: bool = False) -> dict[str, t.Any]:
        # Only use translations
        display_dict: dict[str, t.Any] = {
            "primary": None,
            "secondary": {},
            'vocab': None
        }
        prefix = self._thesaurus['prefix'] if use_prefixes and self._thesaurus and 'prefix' in self._thesaurus and self._thesaurus['prefix'] else ''
        if prefix:
            prefix = f"{prefix}{prefix_separator}"
        if self._thesaurus:
            title = self.thesaurus_title()
            if title:
                display_dict["vocab"] = f"{prefix}{title}"
        if self._mode == "translate":
            if isinstance(self._translations, str):
                display_dict["primary"] = f"{prefix}{self._translations}"
            else:
                if primary_locale in self._translations:
                    display_dict["primary"] = f"{prefix}{self._translations[primary_locale]}"
                elif "und" in self._translations:
                    display_dict["primary"] = f"{prefix}{self._translations['und']}"
                display_dict["secondary"] = {
                    key: f"{prefix}{self._translations[key]}"
                    for key in self._translations
                    if key != "und" and key != primary_locale and self._translations[key]
                }

        # Only use the machine key
        elif self._mode == "value" and not force_translations:
            display_dict["primary"] = f"{prefix}{self._machine_key}"

        # Use a mix of both (machine key as undefined value)
        else:
            display_dict["primary"] = f"{prefix}{self._machine_key}"
            display_dict["secondary"] = {
                key: f"{prefix}{self._translations[key]}"
                for key in self._translations
                if key != "und" and self._translations[key]
            }
        return display_dict

    def key_identifier(self) -> str | None:
        if self._identifier:
            return self._identifier
        if self._machine_key:
            return self._machine_key
        return None

    def thesaurus_title(self) -> str | None:
        if not self._thesaurus:
            return None
        if not ('citation' in self._thesaurus and self._thesaurus['citation']):
            return None
        if not ('title' in self._thesaurus['citation'] and self._thesaurus['citation']['title']):
            return None
        title = self._thesaurus['citation']['title']
        if isinstance(title, str):
            return title
        keys = ['und', 'en']
        keys.extend(title.keys())
        for key in keys:
            if key in title and title[key]:
                return title[key]
        return None

    def thesaurus_group(self):
        if not self._thesaurus:
            return ''
        if 'prefix' in self._thesaurus:
            return self._thesaurus['prefix']
        return self.thesaurus_title() or ''


class KeywordGroup:

    def __init__(self, thesaurus: dict[str, t.Any]):
        self.thesaurus = thesaurus
        self._keywords = {}

    def append(self, keyword: Keyword):
        self._keywords[keyword.key_identifier()] = keyword

    def keywords(self):
        key_names = list(self._keywords.keys())
        key_names.sort()
        for name in key_names:
            yield self._keywords[name]