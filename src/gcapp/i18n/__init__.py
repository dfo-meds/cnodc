"""
This package provides a basic mechanism for translating text.

It works by defining keys that can be looked up in a language-specific dictionary.

In addition, text can be defined as a dictionary of language codes to text and the most
applicable one extracted.

The TranslationManager class stores translations and chooses the appropriate one. It
should be extended to load these translations and select them.

The LanguageDetector class identifies the user's most appropriate language from the
available options.

"""

from gcapp.i18n.base import (
    LanguageDetector,
    MLLink,
    MLString,
    TranslatableError,
    TranslationManager,
    TString,
    dtr,
    format_date,
    i18n_sort,
    tr,
)
from gcapp.i18n.filetm import TomlTranslationManager, YamlTranslationManager

__all__ = (
    "LanguageDetector",
    "MLLink",
    "MLString",
    "TString",
    "TomlTranslationManager",
    "TranslatableError",
    "TranslationManager",
    "YamlTranslationManager",
    "dtr",
    "format_date",
    "i18n_sort",
    "tr",
)
