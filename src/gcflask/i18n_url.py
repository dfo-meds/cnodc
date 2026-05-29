import pathlib
import typing as t

import yaml

import flask
from autoinject import injector
import zirconium as zr
from werkzeug.routing import Rule

from gcflask.i18n import LanguageDetector, TranslationManager


@injector.injectable_global
class PathMapper:

    cfg: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._path_map = {}
        path_map_files = self.cfg.get(("gcflask", "i18n_url_files"), default=[])
        for file_path in path_map_files:
            path_map_file = pathlib.Path(file_path)
            if path_map_file.exists():
                with open(path_map_file, "r", encoding="utf-8") as h:
                    path_map = yaml.safe_load(h) or {}
                    for p in path_map:
                        if p in self._path_map:
                            self._path_map.update(path_map[p])
                        else:
                            self._path_map = path_map[p] or {}

    def get_path_translations(self, path):
        if path in self._path_map and self._path_map[path]:
            for lang in self._path_map[path]:
                yield lang, self._path_map[path][lang]


class MultiLanguageBlueprint(flask.Blueprint):

    mapper: PathMapper = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _register_i18n_route(self, fn, route: str, **kwargs):
        all_langs = []
        for lang, path in self.mapper.get_path_translations(route):
            all_langs.append(lang)
            super().route(path, accept_languages=lang, **kwargs)(fn)
        if all_langs:
            super().route(route, ignore_languages=all_langs, **kwargs)(fn)
        else:
            super().route(route, **kwargs)(fn)

    def route(self, route: str, **kwargs):
        def wrapper(fn):
            self._register_i18n_route(fn, route, **kwargs)
            return fn
        return wrapper


class BilingualRule(Rule):
    """Custom implementation of werkzeug.routing.Rule.

    This implementation accepts the additional accept_languages and ignore_languages parameters which allow the
    selection of an ideal URL based on the URLs it is most suitable for.

    In essence, a Rule which has accept_languages set will ONLY be chosen for those languages. A Rule which has
    ignore_languages set will NEVER be chosen for those languages. In practice, we can use this to make
    language-specific Rules, e.g.:

    @app.route("/accueil", accept_languages="fr")   # Only used when the language is FR
    @app.route("/home", ignore_languages="fr")      # Not used when the language is FR
    def home():
        return "homepage"

    To ensure at least one route is viable for each endpoint, it is recommended to put the default language as one
    which ignores all the other languages which have their URLs set specifically as given in the example above where
    the English URL is the default for any language which is not French.
    """

    ld: LanguageDetector = None
    tm: TranslationManager = None

    @injector.construct
    def __init__(self,
                 *args,
                 accept_languages: str | t.Sequence[str] | None = None,
                 ignore_languages: str | t.Sequence[str] | None = None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._accept_languages: t.Sequence[str] | None = [accept_languages] if isinstance(accept_languages, str) else accept_languages
        self._ignore_languages: t.Sequence[str] | None = [ignore_languages] if isinstance(ignore_languages, str) else ignore_languages

    def suitable_for(self, values: t.Mapping[str, t.Any], method: t.Optional[str] = None) -> bool:
        if not super().suitable_for(values, method):
            return False
        if not (self._ignore_languages or self._accept_languages):
            return True
        lang = "und"
        if "lang" in values:
            lang = values["lang"]
        elif flask.has_request_context():
            lang = self.ld.detect_language(self.tm.supported_languages())
        if self._ignore_languages and lang in self._ignore_languages:
            return False
        return lang in self._accept_languages if self._accept_languages else True
