import pathlib
import typing as t

from autoinject import auto, injector

from dmd.containers.base import Container
from dmd.containers.fields import ChoiceField
from dmd.metadata.registries import BaseRegistry
from gcapp.i18n import MLString


@injector.injectable
class VocabularyRegistry(BaseRegistry):

    def __init__(self):
        super().__init__("vocabularies")

    def vocabulary_options(self) -> list[tuple[str, MLString, MLString]]:
        return [
            (
                key,
                MLString(self[key].get("display", None) or {"und": key}),
                MLString(self[key].get("uri", None) or {})
            )
            for key in self
        ]

    def vocabulary_display(self, vocabulary_name: str) -> MLString:
        return MLString(self[vocabulary_name]["display"])

    def register(self, obj_name, _update_db: bool = True, terms: dict[str, dict] | None = None, **config):
        super().register(obj_name, _update_db, **config)
        if terms is not None:
            self.storage.update_vocabulary_terms(obj_name, terms)

    def register_terms_from_csv(self, vocabulary_name: str, csv_path: pathlib.Path, replace_existing: bool = True):
        import csv
        with open(csv_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            builder = None
            terms = {}
            for line in reader:
                if builder is None:
                    builder = self._term_builder(line)
                else:
                    short_name, data = builder(line)
                    terms[short_name] = data
            self.storage.update_vocabulary_terms(vocabulary_name, terms, replace_existing)

    def _term_builder(self, headers: list[str] | tuple[str, ...]) -> t.Callable[[list[str] | tuple[str, ...]], tuple[str, dict[str, dict]]]:
        short_name: int = -1
        displays: dict[int, str] = {}
        descriptions: dict[int, str] = {}
        und_key: int = -1
        en_key: int = -1
        for idx, key in enumerate(headers):
            key = key.lower().replace(" ", "_")
            if key == "short_name":
                short_name = idx
            elif key.startswith("display__"):
                lang = key[9:]
                displays[idx] = lang
                if lang == "en":
                    en_key = idx
                elif lang == "und":
                    und_key = idx
            elif key.startswith("description__"):
                descriptions[idx] = key[13:]
            else:
                raise ValueError(f"Unknown header: {key}")
        if short_name < 0:
            short_name = und_key if und_key >= 0 else en_key
        if short_name < 0:
            raise ValueError("Missing key header")
        def _builder(row: list[str] | tuple[str, ...]) -> tuple[str, dict[str, dict]]:
            data = {
                "display": {
                    val: row[key]
                    for key, val in displays.items()
                },
                "description": {
                    val: row[key]
                    for key, val in descriptions.items()
                }
            }
            return str(row[short_name]).lower().replace(" ", "_"), data
        return _builder

    def term_options(self, vocabulary_name: str) -> list[tuple[str, MLString, MLString]]:
        return [
            (
                term_name,
                MLString(term_def.get("display", None) or {"und": term_name}),
                MLString(term_def.get("description", None) or {})
            )
            for term_name, term_def in self.storage.load_vocabulary_terms(vocabulary_name)
        ]

    def load_term(self, vocabulary_name: str, short_name: str) -> dict | None:
        return self.storage.load_vocabulary_term(vocabulary_name, short_name)


class VocabularyTerm:

    def __init__(self, term_key: str | None = None, term_label: MLString | None = None):
        self._short_name = term_key
        self._display = term_label or MLString({"und": term_key})

    def __bool__(self):
        return self._short_name is not None

    def short_name(self):
        return self._short_name or ""

    def long_name(self):
        return self._display or ""

    def __getitem__(self, key):
        if key in ("short_name", "short"):
            return self.short_name()
        elif key in ("long_name", "long", "display"):
            return self.long_name()
        else:
            raise KeyError(key)


@injector.construct
class VocabularySelectField(ChoiceField):

    registry: VocabularyRegistry = auto()

    def _build_choices(self) -> list[tuple[str, MLString]]:
        return [
            (short_name, display_name)
            for short_name, display_name, _ in self.registry.term_options(self._config.get("vocabulary_name", ""))
        ]

    def _data_value(self, value: str | None, **kwargs) -> t.Any:
        if value:
            info = self.registry.load_term(self._config.get("vocabulary_name", ""), value)
            if info is not None:
                return VocabularyTerm(value, info.get("display", None))
        if kwargs.get("none_as_blank", False):
            return VocabularyTerm()
        else:
            return None


Container.register_type(VocabularySelectField)
