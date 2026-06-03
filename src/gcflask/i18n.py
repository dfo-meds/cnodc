import abc
import datetime
import typing as t
import markupsafe
from autoinject import injector
import zirconium as zr

from medsutil.exceptions import CodedError


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


class BaseDString(abc.ABC):

    def __bool__(self) -> bool:
        return not self.empty()

    def __add__(self, o: BaseDString) -> BaseDString:
        return DStringComposition([self, o])

    def __mod__(self, args) -> BaseDString:
        if hasattr(args, "keys"):
            return self.format(**args)
        elif hasattr(args, '__iter__') and not isinstance(args, str):
            return self.format(*args)
        else:
            return self.format(args)

    def __copy__(self) -> BaseDString:
        return self.copy()

    def __str__(self) -> str:
        return self.render()

    def __call__(self, **kwargs) -> str:
        return self.render(**kwargs)

    def __html__(self) -> markupsafe.Markup:
        return markupsafe.escape(self.render())

    def render(self, **kwargs) -> str:
        return self._finish_render(self._render(**kwargs))

    def _finish_render(self, x: str) -> str:
        return x

    @abc.abstractmethod
    def empty(self) -> bool:
        raise NotImplementedError

    def copy(self) -> BaseDString:
        return self._build()

    @abc.abstractmethod
    def _render(self, **kwargs) -> str:
        raise NotImplementedError

    def upper(self):
        return self._build(new_transforms=['upper'])

    def lower(self):
        return self._build(new_transforms=['lower'])

    def format(self, *args, **kwargs):
        return self._build(new_args=list(args), new_kwargs=kwargs)

    @abc.abstractmethod
    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        ...


class DStringComposition(BaseDString):

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


class DStringIndividual(BaseDString):

    def __init__(self, *format_args, _transforms: list[str] = None, **format_kwargs):
        self.format_args = list(format_args)
        self.format_kwargs = format_kwargs
        self.transforms = []

    def _finish_render(self, x: str) -> str:
        x = x.format(self.format_args, self.format_kwargs)
        for transform in self.transforms:
            x = getattr(x, transform)()
        return x

    def _update_from(self, x: DStringIndividual, new_args=None, new_kwargs=None, new_transforms=None):
        self.format_args.extend(x.format_args)
        if new_args:
            self.format_args.extend(new_args)
        self.format_kwargs.update(x.format_kwargs)
        if new_kwargs:
            self.format_kwargs.update(new_kwargs)
        self.transforms.extend(x.transforms)
        if new_transforms:
            self.transforms.extend(new_transforms)


class TString(DStringIndividual):

    def __init__(self, text_key: str, default: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text_key = text_key
        self.default = default if default is not None else text_key

    def empty(self) -> bool:
        return self.text_key == '' and self.default == ''

    @injector.inject
    def _render(self, tm: TranslationManager = None, **kwargs):
        return tm.get_text(self.text_key, self.default)

    def _build(self,
               new_args: list | None = None,
               new_kwargs: dict[str, t.Any] | None = None,
               new_transforms: list[str] | None = None):
        new_tstring = TString(self.text_key, self.default)
        new_tstring._update_from(self, new_args, new_kwargs, new_transforms)
        return new_tstring


class MLString(DStringIndividual):

    def __init__(self, language_map: dict | str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.language_map = language_map if isinstance(language_map, dict) else {"und": language_map}

    def empty(self) -> bool:
        return all(not self.language_map[x] for x in self.language_map)

    @injector.inject
    def _render(self, language: str = None, tm: TranslationManager = None, ld: LanguageDetector = None, **kwargs):
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

    def __init__(self, link: str, language_map: dict[str, str], *args, new_tab: bool = False, **kwargs):
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

    def __init__(self, key: str, code_number: int, *, code_space: str | None = None, is_transient: bool = False):
        self.message_key = key
        super().__init__(TString(key), code_number, code_space=code_space, is_transient=is_transient)


def tr(key: str, default: str = "", *args, **kwargs) -> str:
    return t.cast(str, t.cast(object, TString(key, default, *args, **kwargs)))


def format_date(dt: datetime.date | None) -> str:
    if dt is None:
        return tr("gcflask.format.no_date", default="N/A")
    elif isinstance(dt, datetime.datetime):
        return dt.strftime(str(tr("gcflask.format.datetime", default="%Y-%m-%d %H:%M:%S")))
    else:
        return dt.strftime(str(tr("gcflask.format.date", default="%Y-%m-%d")))

def i18n_sort[T](to_sort: t.Iterable[T], *, key=None, reverse=False, insensitive=True, language_order=None) -> t.Iterable[T]:
    if language_order is None:
        language_order = ["und", "en"]
    return sorted(to_sort, key=lambda x: i18n_sort_key(x, key, language_order, insensitive), reverse=reverse)


def i18n_sort_key(x, key, language_order, ignore_case):
    if key is not None:
        if callable(key):
            x = key(x)
        else:
            x = x[key]
    if x is None:
        return ""
    if isinstance(x, str):
        return x if not ignore_case else x.lower()
    elif isinstance(x, t.Mapping):
        for lang in language_order:
            if lang in x and x[lang]:
                return x[lang] if not ignore_case else x[lang].lower()
        for lang in x.keys():
            if x[lang]:
                return x[lang] if not ignore_case else x[lang].lower()
    return ""