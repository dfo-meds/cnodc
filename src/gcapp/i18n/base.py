""" Basic translatable strings """
import abc
import datetime
import typing as t

import markupsafe
import zirconium as zr
from autoinject import injector, auto

from medsutil.exceptions import CodedError


@injector.injectable_global
class TranslationManager:
    """ Stores and looks-up translations as needed. """

    config: zr.ApplicationConfig = auto()
    detector: LanguageDetector = auto()

    @injector.construct
    def __init__(self):
        ...

    def get_text(self, text_key: str, default: str | None = None, _language: str | None = None, _context: str | None = None) -> str:
        """ Retrieves the text in the given language if available, otherwise returns the default.

        :param text_key: The text key to look up
        :param default: The default text to return - if None, the text_key will be used.
        :param _language: The language to look up the text in - if None, the LanguageDetector will be used.
        :param _context: The context to pass to supported_langauges().
        :returns: The translated text, or the default if no text is available.
        """
        return default if default is not None else text_key

    def supported_languages(self, context: str | None = None) -> t.Sequence[str]:
        """ Within a given context, returns a list of supported languages."""
        return ['und']

    def language_options(self, context: str | None = None) -> dict[str, str]:
        """ Within a given context, returns a list of language options to display. """
        return {
            x: self.get_text(f"language.{x}", _language=x, _context=context)
            for x in self.supported_languages(context)
        }


@injector.injectable_global
class LanguageDetector:
    """ Responsible for detecting the user's language preference. """

    def detect_language(self, supported_languages: t.Sequence[str]) -> str:
        """ Given a list of supported languages, returns the most appropriate one for the user. """
        return supported_languages[0] if supported_languages else "und"


class BaseDString(abc.ABC):
    """ Base class for a dynamic string (DString).

        Unlike normal strings, dynamic strings are built from some process (e.g. translation).
        The building is delayed until the string is actually needed somewhere.

        The most important place this is used is when a string is cached [for example, in wtform labels].
        It prevents the translation from being done until the result is actually needed. This lets it adapt
        to the context where it's being printed.
    """

    def __bool__(self) -> bool:
        return not self.empty()

    @abc.abstractmethod
    def empty(self) -> bool:
        """ Checks if the string is empty. """
        raise NotImplementedError

    def __add__(self, o: BaseDString) -> BaseDString:
        return DStringComposition([self, o])

    def __mod__(self, args) -> BaseDString:
        """ Implements str % {dict} or str % {tuple/list} or str % value """
        if hasattr(args, "keys"):
            return self.format(**args)
        elif hasattr(args, '__iter__') and not isinstance(args, str):
            return self.format(*args)
        else:
            return self.format(args)

    def format(self, *args, **kwargs):
        """ Implements str.format() """
        return self._build(new_args=list(args), new_kwargs=kwargs)

    def __copy__(self) -> BaseDString:
        return self.copy()

    def copy(self) -> BaseDString:
        """ Copies the string by rebuilding it. """
        return self._build()

    def __str__(self) -> str:
        return self.render()

    def __call__(self, **kwargs) -> str:
        return self.render(**kwargs)

    def __html__(self) -> markupsafe.Markup:
        """ Supports HTML output by Flask. """
        return markupsafe.escape(self.render())

    def render(self, **kwargs) -> str:
        """ Actually turns the DString into a proper string. """
        return self._finish_render(self._render(**kwargs))

    @abc.abstractmethod
    def _render(self, **kwargs) -> str:
        """ Actually renders the string. """
        raise NotImplementedError

    def _finish_render(self, x: str) -> str:
        return x

    def upper(self):
        return self._build(new_transforms=['upper'])

    def lower(self):
        return self._build(new_transforms=['lower'])

    @abc.abstractmethod
    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        """ Builds a copy of the string with additional arguments, keyword arguments, and transformations. """
        raise NotImplementedError


class DStringComposition(BaseDString):
    """ A composition of DStrings together. """

    def __init__(self, composed: t.Iterable[BaseDString]):
        self._elements = [x for x in composed]

    def empty(self) -> bool:
        return (not self._elements) or all(x.empty() for x in self._elements)

    def _render(self, **kwargs) -> str:
        ret = None
        for x in self._elements:
            rendered = x.render(**kwargs)
            if ret is None:
                ret = rendered
            else:
                ret += rendered
        return ret or ''

    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        new_elements = []
        for x in self._elements:
            new_elements.append(x._build(new_args, new_kwargs, new_transforms))
        return DStringComposition(new_elements)


class FormattableString(BaseDString):
    """ A string that can be formatted """

    def __init__(self, *format_args, _transforms: list[str] = None, **format_kwargs):
        """
        :param format_args: Arguments to render with
        :param _transforms: Transformations to apply (i.e. functions to call on the final string)
        :param format_kwargs: Keyword arguments to render with
        """
        self.format_args = list(format_args)
        self.format_kwargs = format_kwargs
        self.transforms = _transforms or []

    def _finish_render(self, x: str) -> str:
        try:
            x = x.format(*self.format_args, **self.format_kwargs)
            for transform in self.transforms:
                x = getattr(x, transform)()
            return x
        except KeyError as ex:
            ex.add_note("string: " + str(x))
            for idx, x in enumerate(self.format_args):
                ex.add_note(f"arg [{idx}]: {x}")
            for x, y in self.format_kwargs.items():
                ex.add_note(f"kwarg [{x}]: {y}")
            for x in self.transforms:
                ex.add_note(f"transform: {x}")
            raise

    def _update_from(self, x: FormattableString, new_args=None, new_kwargs=None, new_transforms=None):
        self.format_args.extend(x.format_args)
        if new_args:
            self.format_args.extend(new_args)
        self.format_kwargs.update(x.format_kwargs)
        if new_kwargs:
            self.format_kwargs.update(new_kwargs)
        self.transforms.extend(x.transforms)
        if new_transforms:
            self.transforms.extend(new_transforms)


class TString(FormattableString):
    """ A string that should be translated. """

    def __init__(self, text_key: str, default: str | None = None, *args, **kwargs):
        """
        :param text_key: The text_key to pass to TranslationManager
        :param default: The default text to use
        :param args: Formatting arguments (see FormattableString)
        :param kwargs: Formatting keyword arguments (see FormattableString)
        """
        super().__init__(*args, **kwargs)
        self.text_key = text_key
        self.default = default if default is not None else text_key

    def empty(self) -> bool:
        return self.text_key == '' and self.default == ''

    @injector.inject
    def _render(self, tm: TranslationManager = auto(), **kwargs):
        return tm.get_text(self.text_key, self.default)

    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        new_tstring = TString(self.text_key, self.default)
        new_tstring._update_from(self, new_args, new_kwargs, new_transforms)
        return new_tstring


class MLString(FormattableString):
    """ A multilanguage string, which is actually a dictionary of strings keyed by language options. """

    def __init__(self, language_map: dict | str, *args, **kwargs):
        """
        :param language_map: A map of language keys to translated strings.
        :param args: Formatting arguments
        :param kwargs: Formatting keyword arguments (see FormattableString)
        """
        super().__init__(*args, **kwargs)
        self.language_map = language_map if isinstance(language_map, dict) else {"und": language_map}

    def empty(self) -> bool:
        return all(not self.language_map[x] for x in self.language_map)

    @injector.inject
    def _render(self, language: str = None, ld: LanguageDetector = auto(), **kwargs):
        lang_opts = list(self.language_map.keys())
        lang = language if language is not None and language in lang_opts else ld.detect_language(lang_opts)
        priority_order = [lang, "und", "en", "fr"]
        priority_order.extend(self.language_map.keys())
        for cl in priority_order:
            if cl in self.language_map and self.language_map[cl]:
                return self.language_map[cl]
        return ""

    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        new_mlstring = MLString(self.language_map)
        new_mlstring._update_from(self, new_args, new_kwargs, new_transforms)
        return new_mlstring

    def keys(self):
        keys = []
        for key in self.language_map:
            if self.language_map[key]:
                keys.append(key)
        return keys

    def __len__(self):
        return len(self.language_map)

    def __getitem__(self, key):
        if isinstance(key, str) and key in self.language_map:
            return self.render(language=key)
        raise KeyError(key)

    def __iter__(self):
        return iter(self.keys())

    def __contains__(self, key):
        return key in self.language_map and self.language_map[key]


class MLLink(MLString):
    """ An HTML link with a multilingual label. """

    def __init__(self, link: str, language_map: dict[str, str], *args, new_tab: bool = False, **kwargs):
        """
        :param link: The HTML link
        :param language_map: The language map of language keys to translated strings.
        :param args: Arguments for formatting
        :param new_tab: Whether to open it in a new tab
        :param kwargs: Keyword arguments for formatting
        """
        self.link = link
        self.new_tab = new_tab
        super().__init__(language_map, *args, **kwargs)

    def _render(self, **kwargs):
        text = super().render(**kwargs)
        target = "" if not self.new_tab else " target='_blank'"
        return markupsafe.Markup(f'<a href="{self.link}"{target}>{markupsafe.escape(text)}</a>')

    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        new_mlstring = MLLink(self.link, self.language_map, new_tab=self.new_tab)
        new_mlstring._update_from(self, new_args, new_kwargs, new_transforms)
        return new_mlstring


class TranslatableError(CodedError):
    """ An error which can be translated. """

    def __init__(self, key: str, code_number: int | None = None, *, code_space: str | None = None, is_transient: bool = False):
        """ As per CodedError, but with a text key instead of a message. """
        self.message_key = key
        super().__init__(tr(key), code_number, code_space=code_space, is_transient=is_transient)


def tr(key: str, default: str | None = None, *args, _delay: bool = False, **kwargs) -> str:
    """ Translates the given key into text.

        Care must be taken when using _delay - it works well-enough for most cases, but
        the object returned is not actually a string (but it works a lot like a string).

        :param key: The key to translate
        :param default: The default value to return if the result isn't defined
        :param args: Arguments for formatting
        :param _delay: If true, actually returns TString
        :param kwargs: Keyword arguments for formatting
    """
    tstr = TString(key, default, *args, **kwargs)
    if _delay:
        return tstr
    else:
        return str(tstr)


def dtr(key: str, default: str | None = None, *args, **kwargs) -> str:
    """ Wrapper around tr() that sets _delay to True. """
    return tr(key, default, *args, **kwargs, _delay=True)


def format_date(dt: datetime.date | None) -> str:
    """ Formats the given date as a string.

        The format used is loaded from the translation manager:
        - gcapp.format.no_date is used for null dates (default N/A)
        - gcapp.format.datetime is used for datetimes (default Y-m-d H:M:S)
        - gcapp.format.date is used for dates (default is Y-m-d)
    """
    if dt is None:
        return tr("gcapp.format.no_date", default="N/A")
    elif isinstance(dt, datetime.datetime):
        return dt.strftime(str(tr("gcapp.format.datetime", default="%Y-%m-%d %H:%M:%S")))
    else:
        return dt.strftime(str(tr("gcapp.format.date", default="%Y-%m-%d")))


def i18n_sort[T](to_sort: t.Iterable[T],
                 *,
                 key: str | int | t.Callable | None = None,
                 reverse: bool = False,
                 insensitive: bool = True,
                 language_order: t.Sequence[str] | None = None) -> list[T]:
    """ Sorts the iterable into a list of items - see i18n_sort_key for how the sort order is chosen. """
    return sorted(to_sort, key=lambda x: i18n_sort_key(
        x,
        key,
        language_order or ["und", "en"],
        insensitive
    ), reverse=reverse)


def i18n_sort_key[T](x: T,
                     key: t.Callable | str | int | None,
                     language_order: t.Sequence[str],
                     ignore_case: bool) -> str:
    """ Retrieves an appropriate sort key for the item.

        The sort key is determined by:
        - if key is specified:
          - if key is callable, it will be called with x as an argument and the result treated as x
          - otherwise, it will be used as an index on x to get a new value for x
        - if x is a mapping
          - if any of the entries in language_order is set and non-empty, the value of x is set to the first one
          - otherwise one of the keys of x is chosen
        - if ignore_case is specified (and x is not None), then x is lower-cased
        - the result is the sort key

        :param x: The item to get a sort key for
        :param key: Works like key from list.sort() or sorted(), either a callable that takes x and gives a key, or an
          index that can be used with x.
        :param language_order: If x is an MLString, the first non-empty value will be used.
        :param ignore_case: If true, the result will be lower-cased.

    """
    if key is not None:
        if callable(key):
            x = key(x)
        else:
            x = x[key]
    if x is None:
        return ""
    if isinstance(x, t.Mapping) and not isinstance(x, str):
        for lang in language_order:
            if lang in x and x[lang]:
                x = x[lang]
                break
        else:
            for lang in x.keys():
                if x[lang]:
                    x = x[lang]
                    break
            else:
                x = ""
    return x.lower() if ignore_case else x
