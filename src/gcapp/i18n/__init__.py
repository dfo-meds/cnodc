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
    TranslationManager,
    LanguageDetector,
    TString,
    MLString,
    MLLink,
    TranslatableError,
    tr,
    dtr,
    format_date,
    i18n_sort
)
from gcapp.i18n.filetm import YamlTranslationManager, TomlTranslationManager
