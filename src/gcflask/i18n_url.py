""" Support for translated routes in Flask.

    Flask routes can be documented in a YAML file (listed under :confval:`gcflask.i18n_url_files`).

    The special blueprint class py:class:`gcflask.i18n_url.MultiLanguageBlueprint` checks for
    available translations and registers the same handler under all blueprints found. It also
    tracks the language as a URL parameter so that py:func:`flask.url_for` will return the correct
    URL for the current language.

"""
import pathlib
import typing as t

import yaml
import flask
from autoinject import injector, auto
import zirconium as zr
from werkzeug.routing import Rule

from gcapp.i18n.base import TranslationManager, LanguageDetector


@injector.injectable_global
class PathMapper:
    """ A class responsible for mapping route names. """

    cfg: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self):
        self._path_map: dict[str, dict[str, str]] = {}
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

    def get_path_translations(self, path: str) -> t.Generator[tuple[str, str], None, None]:
        """ Iterates over all the pairs of languages and translated paths for a given path. """
        if path in self._path_map and self._path_map[path]:
            for lang in self._path_map[path]:
                yield lang, self._path_map[path][lang]


class MultiLanguageBlueprint(flask.Blueprint):
    """ A derivative of Blueprint that handles multilingual path mapping. """

    mapper: PathMapper = auto()

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _register_i18n_route(self, fn: t.Callable, route: str, **kwargs):
        """ Registers a translatable route. """
        all_langs = []

        # handle all the languages that have a specific translation
        for lang, path in self.mapper.get_path_translations(route):
            all_langs.append(lang)
            super().route(path, accept_languages=lang, **kwargs)(fn)

        # if any specific translations were registered, we use ignore_languages to ignore them
        if all_langs:
            super().route(route, ignore_languages=all_langs, **kwargs)(fn)

        # otherwise, this is just a normal registration
        else:
            super().route(route, **kwargs)(fn)

    def route[**R, S](self, route: str, **kwargs) -> t.Callable[[t.Callable[R, S]], t.Callable[R, S]]:
        """ Overrides the decorator for route() by checking for any translated paths as well. """
        def wrapper[**P, Q](fn: t.Callable[P, Q]) -> t.Callable[P, Q]:
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

    ld: LanguageDetector = auto()
    tm: TranslationManager = auto()

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

        # must be suitable according to the normal definition
        if not super().suitable_for(values, method):
            return False

        # if we didn't specify any languages to ignore or accept, we can just return true
        if not (self._ignore_languages or self._accept_languages):
            return True

        # get the current language, either from values (which is the arguments for url_for) ot from the language detector.
        lang = "und"
        if "lang" in values:
            lang = values["lang"]
        elif flask.has_request_context():
            lang = self.ld.detect_language(self.tm.supported_languages())

        # if we are ignoring it, return false
        if self._ignore_languages and lang in self._ignore_languages:
            return False

        return lang in self._accept_languages if self._accept_languages else True
