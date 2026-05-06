import typing as t

import flask
from autoinject import injector
import zirconium as zr


@injector.injectable_global
class LanguageDetector:

    def detect_language(self, supported_languages: t.Sequence[str]) -> str:
        return supported_languages[0] if supported_languages else "und"


@injector.injectable_global
class TranslationManager:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self): ...

    def get_text(self, text_key: str, default: str = None) -> str:
        return default if default is not None else text_key

    def supported_languages(self, context: str = "interface"):
        return ['und']
