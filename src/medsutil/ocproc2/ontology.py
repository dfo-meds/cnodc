import pathlib
import threading
import typing as t
import urllib.parse

from autoinject import injector

SKOS_IN_SCHEME = "http://www.w3.org/2004/02/skos/core#inScheme"
CNODC_PREFIX = "http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/cnodc.ttl#"
IOOS_PREFIX = "http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/ioos.ttl#"
EOV_PREFIX = "http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/eov.ttl#"
RS_PREFIX = "http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/rstypes.ttl#"
CNODC_ELEMENTS = f"{CNODC_PREFIX}elements"
CNODC_RECORDSET_TYPES = f"{RS_PREFIX}recordSetTypes"
CNODC_PREF_UNIT = f"{CNODC_PREFIX}preferredUnit"
CNODC_DATA_TYPE = f"{CNODC_PREFIX}dataType"
CNODC_GROUP = f"{CNODC_PREFIX}elementGroup"
CNODC_ALLOW_MULTI = f"{CNODC_PREFIX}allowMulti"
CNODC_MIN = f"{CNODC_PREFIX}minValue"
CNODC_MAX = f"{CNODC_PREFIX}maxValue"
CNODC_ALLOW = f"{CNODC_PREFIX}allowedValue"
CNODC_COORDINATE = f"{RS_PREFIX}requireCoordinate"
CNODC_IOOS_CATEGORY = f"{CNODC_PREFIX}ioosCategory"
CNODC_IGNORE_IN_DUPES = f"{CNODC_PREFIX}ignoreInDuplicateCheck "
CNODC_EOV = f"{CNODC_PREFIX}essentialOceanVariable"
SKOS_LABEL = "http://www.w3.org/2004/02/skos/core#prefLabel"
SKOS_DOCUMENTATION = "http://www.w3.org/2004/02/skos/core#documentation"


if t.TYPE_CHECKING:
    import rdflib.term

    LiteralValue = set[rdflib.term.Literal] | rdflib.term.Literal
    ReferenceValue = str | set[str]


class _BaseInfo:
    __slots__ = ("_documentation", "_label", "name")

    def __init__(self, name):
        self.name = name
        self._label = {}
        self._documentation = {}

    def _default_label(self) -> str:
        x = ""
        for char in self.name:
            if x == "":
                x += char
            else:
                if char.isupper() and not x[-1].isupper():
                    x += " "
                x += char
        return x

    def label(self, lang: str = "en", default=None) -> str:
        return _BaseInfo._get_language_attribute(
            self._label, lang, default if default is not None else self._default_label()
        )

    def set_label(self, label: LiteralValue):
        for lang, value in _BaseInfo.build_all_from_multilingual(label):
            self._label[lang] = value

    def documentation(self, lang: str = "en", default="") -> str:
        return _BaseInfo._get_language_attribute(self._documentation, lang, default)

    def set_documentation(self, doc: LiteralValue):
        for lang, value in _BaseInfo.build_all_from_multilingual(doc):
            self._documentation[lang] = value

    @staticmethod
    def _get_language_attribute(
        lang_attr: dict, lang: str = "en", default: str = ""
    ) -> str:
        if lang in lang_attr and lang_attr[lang] != "":
            return lang_attr[lang]
        elif "und" in lang_attr and lang_attr["und"] != "":
            return lang_attr["und"]
        else:
            return default

    @staticmethod
    def build_all_from_multilingual(
        literal: LiteralValue,
    ) -> t.Iterable[tuple[str, str]]:
        if isinstance(literal, set):
            for lit in literal:
                yield (lit.language or "und"), _BaseInfo.unquote(lit.value)
        else:
            yield literal.language or "und", _BaseInfo.unquote(literal.value)

    @staticmethod
    def build_one_from_ref(ref_value: ReferenceValue) -> str:
        if isinstance(ref_value, set):
            return _BaseInfo._remove_prefix(
                next(iter(ref_value))
            )  # pragma: no coverage (this is unused usually unless there's a major issue)
        else:
            return _BaseInfo._remove_prefix(ref_value)

    @staticmethod
    def build_all_from_ref(ref_value: ReferenceValue) -> t.Iterable[str]:
        if isinstance(ref_value, set):
            for x in ref_value:
                yield _BaseInfo._remove_prefix(x)
        else:
            yield _BaseInfo._remove_prefix(ref_value)

    @staticmethod
    def build_one_from_literal(ref_value: LiteralValue) -> str | int | float:
        if isinstance(ref_value, set):
            return _BaseInfo.unquote(
                next(iter((ref_value))).value
            )  # pragma: no coverage (this is unused usually unless there's a major issue)
        else:
            return _BaseInfo.unquote(ref_value.value)

    @staticmethod
    def build_all_from_literal(
        ref_value: LiteralValue,
    ) -> t.Iterable[str | int | float]:
        if isinstance(ref_value, set):
            for x in ref_value:
                yield _BaseInfo.unquote(x.value)
        else:
            yield _BaseInfo.unquote(ref_value.value)

    @staticmethod
    def unquote(x):
        if isinstance(x, str):
            return urllib.parse.unquote(x)
        return x

    @t.overload
    @staticmethod
    def _remove_prefix(x: str) -> str: ...

    @t.overload
    @staticmethod
    def _remove_prefix(x: None) -> None: ...

    @staticmethod
    def _remove_prefix(x: str | None) -> str | None:
        if x is None:
            return x  # pragma: no coverage (this is unused usually unless there's a major issue)
        else:
            return x if "#" not in x else x[x.rfind("#") + 1 :]


class OCProc2ChildRecordTypeInfo(_BaseInfo):
    __slots__ = ("_documentation", "_label", "coordinates", "name")

    def __init__(self, name: str):
        self.coordinates: list[set[str]] = []
        super().__init__(name)

    def update_coordinates(self, coords: LiteralValue):
        for x in self.build_all_from_literal(coords):
            if isinstance(x, str):
                self.coordinates.append(set(x.split("|")))
            else:
                raise TypeError("Invalid type for coords")


class OCProc2ElementInfo(_BaseInfo):
    __slots__ = (
        "_documentation",
        "_label",
        "allow_many",
        "allowed_values",
        "data_type",
        "essential_ocean_vars",
        "group_name",
        "ignore_for_dedupe",
        "ioos_category",
        "max_value",
        "min_value",
        "name",
        "preferred_unit",
    )

    def __init__(
        self,
        name: str,
        allow_multi: bool = False,
        min_value: float | None = None,
        max_value: float | None = None,
        data_type: str | None = None,
        preferred_unit: str | None = None,
        groups: str | None = None,
        allowed_values: set[int | str | float] | None = None,
        ioos_category: str | None = None,
        essential_ocean_variables: set[str] | None = None,
        ignore_for_dedupe: bool = False,
    ):
        self.group_name: str | None = groups
        self.preferred_unit = preferred_unit
        self.data_type = data_type
        self.allow_many = allow_multi
        self.min_value = min_value
        self.max_value = max_value
        self.ignore_for_dedupe = ignore_for_dedupe
        self.allowed_values = allowed_values or None
        self.ioos_category = ioos_category or None
        self.essential_ocean_vars = essential_ocean_variables or set()
        super().__init__(name)

    def set_ioos_category(self, ioos_category: ReferenceValue):
        self.ioos_category = _BaseInfo.build_one_from_ref(ioos_category)

    def update_essential_ocean_vars(self, ocean_var: ReferenceValue):
        self.essential_ocean_vars.update(_BaseInfo.build_all_from_ref(ocean_var))

    def set_ignore_for_dedupe(self, ignore_for_dedupe: LiteralValue):
        self.ignore_for_dedupe = (
            _BaseInfo.build_one_from_literal(ignore_for_dedupe) == "True"
        )

    def set_preferred_unit(self, unit: LiteralValue):
        self.preferred_unit = _BaseInfo.build_one_from_literal(unit)

    def set_data_type(self, data_type: ReferenceValue):
        self.data_type = _BaseInfo.build_one_from_ref(data_type)

    def set_allowed_group(self, group: LiteralValue):
        self.group_name = str(_BaseInfo.build_one_from_literal(group))

    def update_allowed_values(self, avs: LiteralValue):
        if self.allowed_values is None:
            self.allowed_values = set()
        self.allowed_values.update(_BaseInfo.build_all_from_literal(avs))

    def set_allow_multi(self, allow_multi: LiteralValue):
        self.allow_many = (
            str(_BaseInfo.build_one_from_literal(allow_multi)).lower().strip()
            != "false"
        )

    def set_min_value(self, min_value: LiteralValue):
        self.min_value = _BaseInfo.build_one_from_literal(min_value)

    def set_max_value(self, max_value: LiteralValue):
        self.max_value = _BaseInfo.build_one_from_literal(max_value)


@injector.injectable_global
class OCProc2Ontology:
    def __init__(self, ontology_file: pathlib.Path | None = None):
        self._onto_files = []
        if ontology_file:
            self._onto_files.append(ontology_file)
        else:
            from medsutil import ROOT_DIR

            vocab_dir = ROOT_DIR / "vocab"
            self._onto_files.append(vocab_dir / "eov.ttl")
            self._onto_files.append(vocab_dir / "ioos.ttl")
            self._onto_files.append(vocab_dir / "cnodc.ttl")
            self._onto_files.append(vocab_dir / "rstypes.ttl")
        self._parameters: dict[str, OCProc2ElementInfo] | None = None
        self._recordset_types: dict[str, OCProc2ChildRecordTypeInfo] | None = None
        self._load_lock = threading.Lock()
        self._load_graph()

    def _load_graph(self):
        if self._parameters is None:
            with self._load_lock:
                if self._parameters is None:
                    import rdflib

                    self._parameters = {}
                    self._recordset_types = {}
                    graph = rdflib.Graph()
                    for f in self._onto_files:
                        graph.parse(str(f))
                    graph_dict: dict[str, dict] = {}
                    for a, b, c in graph:
                        a = str(a)
                        b = str(b)
                        if a not in graph_dict:
                            graph_dict[a] = {}
                        if b in graph_dict[a]:
                            if isinstance(graph_dict[a][b], str):
                                graph_dict[a][b] = {graph_dict[a][b]}
                            graph_dict[a][b].add(c)
                        else:
                            graph_dict[a][b] = c
                    for key, value in graph_dict.items():
                        if SKOS_IN_SCHEME not in value:
                            continue
                        if str(value[SKOS_IN_SCHEME]) == CNODC_ELEMENTS:
                            e_name = key[key.rfind("#") + 1 :]
                            self._parameters[e_name] = OCProc2ElementInfo(e_name)
                            if SKOS_LABEL in value:
                                self._parameters[e_name].set_label(
                                    value[SKOS_LABEL]
                                )
                            if SKOS_DOCUMENTATION in value:
                                self._parameters[e_name].set_documentation(
                                    value[SKOS_DOCUMENTATION]
                                )
                            if CNODC_PREF_UNIT in value:
                                self._parameters[e_name].set_preferred_unit(
                                    value[CNODC_PREF_UNIT]
                                )
                            if CNODC_DATA_TYPE in value:
                                self._parameters[e_name].set_data_type(
                                    value[CNODC_DATA_TYPE]
                                )
                            if CNODC_GROUP in value:
                                self._parameters[e_name].set_allowed_group(
                                    value[CNODC_GROUP]
                                )
                            if CNODC_ALLOW_MULTI in value:
                                self._parameters[e_name].set_allow_multi(
                                    value[CNODC_ALLOW_MULTI]
                                )
                            if CNODC_MIN in value:
                                self._parameters[e_name].set_min_value(
                                    value[CNODC_MIN]
                                )
                            if CNODC_MAX in value:
                                self._parameters[e_name].set_max_value(
                                    value[CNODC_MAX]
                                )
                            if CNODC_ALLOW in value:
                                self._parameters[e_name].update_allowed_values(
                                    value[CNODC_ALLOW]
                                )
                            if CNODC_EOV in value:
                                self._parameters[e_name].update_essential_ocean_vars(
                                    value[CNODC_EOV]
                                )
                            if CNODC_IOOS_CATEGORY in value:
                                self._parameters[e_name].set_ioos_category(
                                    value[CNODC_IOOS_CATEGORY]
                                )
                            if CNODC_IGNORE_IN_DUPES in value:
                                self._parameters[e_name].set_ignore_for_dedupe(
                                    value[CNODC_IGNORE_IN_DUPES]
                                )
                        elif (
                            str(value[SKOS_IN_SCHEME])
                            == CNODC_RECORDSET_TYPES
                        ):
                            e_name = key[key.rfind("#") + 1 :]
                            self._recordset_types[e_name] = OCProc2ChildRecordTypeInfo(
                                key
                            )
                            if SKOS_LABEL in value:
                                self._recordset_types[e_name].set_label(
                                    value[SKOS_LABEL]
                                )
                            if SKOS_DOCUMENTATION in value:
                                self._recordset_types[e_name].set_documentation(
                                    value[SKOS_DOCUMENTATION]
                                )
                            if CNODC_COORDINATE in value:
                                self._recordset_types[e_name].update_coordinates(
                                    value[CNODC_COORDINATE]
                                )

    def recordset_info(
        self, recordset_type_name: str
    ) -> OCProc2ChildRecordTypeInfo | None:
        if recordset_type_name in self._recordset_types:
            return self._recordset_types[recordset_type_name]
        return None

    def recordset_exists(self, recordset_type: str) -> bool:
        return recordset_type in self._recordset_types

    def coordinates(self, recordset_type: str) -> set | None:
        if recordset_type in self._recordset_types:
            return self._recordset_types[recordset_type].coordinates or None
        return None

    def info(self, element_name: str) -> OCProc2ElementInfo | None:
        if element_name in self._parameters:
            return self._parameters[element_name]
        return None

    def allow_many(self, element_name: str) -> bool | None:
        if element_name in self._parameters:
            return self._parameters[element_name].allow_many
        return None

    def preferred_unit(self, element_name: str) -> str | None:
        if element_name in self._parameters:
            return self._parameters[element_name].preferred_unit
        return None

    def group_name(self, element_name: str) -> str | None:
        if element_name in self._parameters:
            return self._parameters[element_name].group_name
        return None

    def data_type(self, element_name: str) -> str | None:
        if element_name in self._parameters:
            return self._parameters[element_name].data_type
        return None

    def exists(self, element_name: str) -> bool:
        return element_name in self._parameters

    def min_value(self, element_name: str) -> float | int | None:
        if element_name in self._parameters:
            return self._parameters[element_name].min_value
        return None

    def max_value(self, element_name: str) -> float | int | None:
        if element_name in self._parameters:
            return self._parameters[element_name].max_value
        return None

    def allowed_values(
        self, element_name: str
    ) -> set[str | int | float] | None:
        if element_name in self._parameters:
            return self._parameters[element_name].allowed_values or None
        return None

    def ioos_category(self, element_name: str) -> str | None:
        if element_name in self._parameters:
            return self._parameters[element_name].ioos_category
        return None

    def essential_ocean_vars(self, element_name: str) -> set[str] | None:
        if element_name in self._parameters:
            return self._parameters[element_name].essential_ocean_vars or None
        return None
