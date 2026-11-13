import typing as t

from autoinject import injector
from zirconium import ApplicationConfig

from gcapp.i18n.base import LanguageDetector
from medsutil.ocproc2 import OCProc2Ontology


class DesktopLanguageDetector(LanguageDetector):
    config: ApplicationConfig

    @injector.construct
    def __init__(self):
        self._current_language: str = self.config.as_str("language", default="und")

    def set_language(self, language: str):
        self._current_language = language

    def detect_language(self, supported_languages: t.Sequence[str]) -> str:
        return (
            self._current_language
            if self._current_language in supported_languages
            else "und"
        )


@injector.injectable
class OCProc2Translator:
    detector: LanguageDetector
    ontology: OCProc2Ontology

    ELEMENT_TYPE_NAMES: t.ClassVar[dict[str, dict[str, str]]] = {
        "metadata": {
            "en": "Metadata",
            "fr": "Métadonnées",
        },
        "coordinates": {
            "en": "Coordinates",
            "fr": "Coordonnées",
        },
        "parameters": {
            "en": "Parameters",
            "fr": "Paramètres",
        },
    }

    @injector.construct
    def __init__(self): ...

    def translate_recordset_type(self, rs_type: str) -> str:
        info = self.ontology.recordset_info(rs_type)
        if info is not None:
            return info.label(self.detector.detect_language(["en", "fr"]))
        else:
            return (" ".join(rs_type.split("_"))).lower().title()

    def translate_element_name(self, element_name: str) -> str:
        info = self.ontology.info(element_name)
        if info is not None:
            return info.label(self.detector.detect_language(["en", "fr"]))
        else:
            return element_name

    def translate_element_type(self, element_type: str) -> str:
        lang = self.detector.detect_language(["en", "fr"])
        if (
            element_type in self.ELEMENT_TYPE_NAMES
            and lang in self.ELEMENT_TYPE_NAMES[element_type]
        ):
            return self.ELEMENT_TYPE_NAMES[element_type][lang]
        else:
            return element_type.title()

    def translate_element_description(self, element_name: str) -> str:
        info = self.ontology.info(element_name)
        if info is not None:
            return info.documentation(self.detector.detect_language(["en", "fr"]))
        else:
            return ""
