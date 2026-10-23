""" Useful overrides of TranslationManager. """
import typing as t
import pathlib

from gcapp.i18n.base import TranslationManager


class FileTranslationManager(TranslationManager):
    """ Base class for a file-based translation manager where the file is a format that supports maps. """

    def __init__(self, suffix: str, extra_paths: list[str] | None = None):
        """
        :param suffix: The suffix of translation files.
        :param extra_paths: A list of paths to search for translation files.
        """
        self._suffix = suffix
        super().__init__()
        self._dictionaries: dict[str, dict[str, str]] = {}
        for path in self._get_paths(extra_paths):
            self._scan_for_dictionaries(path)

    def _get_paths(self, extra_paths: list[str] | None) -> t.Generator[pathlib.Path, None, None]:
        """ Generates a list of paths to search for translation files.

            Translation paths may come from extra_paths or from
            configuration gcapp.translations.paths.
        """
        if extra_paths:
            for path in extra_paths:
                yield pathlib.Path(path).resolve().absolute()
        config_paths = self.config.as_list(("gcapp", "translations", "paths"))
        if config_paths:
            for path in config_paths:
                yield pathlib.Path(path).resolve().absolute()

    def _scan_for_dictionaries(self, path: pathlib.Path):
        """ Searches a directory for dictionary files. """
        if not path.exists():
            return
        if path.is_file() and path.name.endswith(f".{self._suffix}"):
            self._update_dictionary(path.stem, self._load_dictionary_file(path))
        else:
            for p in path.glob(f"*.{self._suffix}"):
                if p.is_file() and p.name.endswith(f".{self._suffix}"):
                    self._update_dictionary(p.stem, self._load_dictionary_file(p))

    def _load_dictionary_file(self, path: pathlib.Path) -> dict[str, str | dict]:
        """ Loads a dictionary from the file. """
        raise NotImplementedError

    def _update_dictionary(self, language: str, content: dict[str, str | dict]):
        """ Updates the language dictionary with the content."""
        if language not in self._dictionaries:
            self._dictionaries[language] = self._collapse_dictionary(content)
        else:
            self._dictionaries[language].update(self._collapse_dictionary(content))

    def _collapse_dictionary(self, content: dict, prefix: str = "") -> dict[str, str]:
        """ Collapses nested dictionaries into the flat structure.

            For example (in YAML) ::

                gcapp:
                    common:
                        placeholder: Placeholder

            Becomes ::

                gcapp.common.placeholder: Placeholder
        """
        actual = {}
        for key in content.keys():
            if isinstance(content[key], dict):
                actual.update(self._collapse_dictionary(content[key], f"{prefix}{key}."))
            else:
                actual[prefix + key] = str(content[key])
        return actual

    def get_text(self, text_key: str, default: str | None = None, _language: str | None = None, _context: str | None = None):

        if _language is None:
            _language = self.detector.detect_language(self.supported_languages(_context))

        if _language is not None:
            if _language in self._dictionaries and text_key in self._dictionaries[_language]:
                return self._dictionaries[_language][text_key]

        return default if default is not None else text_key

    def supported_languages(self, context: str | None = None) -> list[str]:
        return list(self._dictionaries.keys())


class TomlTranslationManager(FileTranslationManager):
    """ Implementation of FileTranslationManager for TOML files. """

    def __init__(self, extra_paths: list[str] | None = None):
        super().__init__("toml", extra_paths)

    def _load_dictionary_file(self, path: pathlib.Path) -> dict[str, str | dict]:
        import tomllib
        with open(path, "rb") as h:
            return tomllib.load(h)


class YamlTranslationManager(FileTranslationManager):
    """ Implementation of FileTranslationManager for YAML files. """

    def __init__(self, extra_paths: list[str] | None = None):
        super().__init__("yaml", extra_paths)

    def _load_dictionary_file(self, path: pathlib.Path) -> dict[str, str | dict]:
        import yaml
        with open(path, "rb") as h:
            return yaml.safe_load(h)
