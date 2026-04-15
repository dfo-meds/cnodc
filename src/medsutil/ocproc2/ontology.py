import rdflib
import pathlib
import threading
import typing as t

import rdflib.term
from autoinject import injector


SKOS_IN_SCHEME = 'http://www.w3.org/2004/02/skos/core#inScheme'
CNODC_PREFIX = 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/cnodc.ttl#'
IOOS_PREFIX = 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/ioos.ttl#'
EOV_PREFIX = 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/eov.ttl#'
RS_PREFIX = 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/rstypes.ttl#'
CNODC_ELEMENTS = f'{CNODC_PREFIX}elements'
CNODC_RECORDSET_TYPES = f'{RS_PREFIX}recordSetTypes'
CNODC_PREF_UNIT = f'{CNODC_PREFIX}preferredUnit'
CNODC_DATA_TYPE = f'{CNODC_PREFIX}dataType'
CNODC_GROUP = f'{CNODC_PREFIX}elementGroup'
CNODC_ALLOW_MULTI = f'{CNODC_PREFIX}allowMulti'
CNODC_MIN = f'{CNODC_PREFIX}minValue'
CNODC_MAX = f'{CNODC_PREFIX}maxValue'
CNODC_ALLOW = f'{CNODC_PREFIX}allowedValue'
CNODC_COORDINATE = f'{RS_PREFIX}requireCoordinate'
CNODC_IOOS_CATEGORY = f'{CNODC_PREFIX}ioosCategory'
CNODC_EOV = f'{CNODC_PREFIX}essentialOceanVariable'
SKOS_LABEL = f'http://www.w3.org/2004/02/skos/core#prefLabel'
SKOS_DOCUMENTATION = f'http://www.w3.org/2004/02/skos/core#documentation'


LiteralValue = t.Union[set[rdflib.term.Literal], rdflib.term.Literal]
ReferenceValue = t.Union[str, set[str]]

class _BaseInfo:

    __slots__ = ('name', '_label', '_documentation')

    def __init__(self, name):
        self.name = name
        self._label = {}
        self._documentation = {}

    def label(self, lang: str = 'en') -> str:
        return _BaseInfo._get_language_attribute(self._label, lang)

    def set_label(self, label: LiteralValue):
        for lang, value in _BaseInfo.build_all_from_multilingual(label):
            self._label[lang] = value

    def documentation(self, lang: str = 'en') -> str:
        return _BaseInfo._get_language_attribute(self._documentation, lang)

    def set_documentation(self, doc: LiteralValue):
        for lang, value in _BaseInfo.build_all_from_multilingual(doc):
            self._documentation[lang] = value

    @staticmethod
    def _get_language_attribute(lang_attr: dict, lang: str = 'en', default: str = '') -> str:
        if lang in lang_attr and lang_attr[lang] != '':
            return lang_attr[lang]
        elif 'und' in lang_attr and lang_attr['und'] != '':
            return lang_attr['und']
        else:
            return default

    @staticmethod
    def build_all_from_multilingual(literal: LiteralValue) -> t.Iterable[tuple[str, str]]:
        if isinstance(literal, set):
            for lit in literal:
                yield (lit.language or 'und'), lit.value
        else:
            yield literal.language or 'und', literal.value

    @staticmethod
    def build_one_from_ref(ref_value: ReferenceValue) -> str:
        if isinstance(ref_value, set):
            return _BaseInfo._remove_prefix(list(ref_value)[0]) # pragma: no coverage (this is unused usually unless there's a major issue)
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
    def build_one_from_literal(ref_value: LiteralValue) -> t.Union[str, int, float]:
        if isinstance(ref_value, set):
            return list(ref_value)[0].value # pragma: no coverage (this is unused usually unless there's a major issue)
        else:
            return ref_value.value

    @staticmethod
    def build_all_from_literal(ref_value: LiteralValue) -> t.Iterable[str | int | float]:
        if isinstance(ref_value, set):
            for x in ref_value:
                yield x.value
        else:
            yield ref_value.value

    @t.overload
    @staticmethod
    def _remove_prefix(x: str) -> str: ...

    @t.overload
    @staticmethod
    def _remove_prefix(x: None) -> None: ...

    @staticmethod
    def _remove_prefix(x: str | None) -> str | None:
        if x is None:
            return x    # pragma: no coverage (this is unused usually unless there's a major issue)
        else:
            return x if '#' not in x else x[x.rfind('#')+1:]


class OCProc2ChildRecordTypeInfo(_BaseInfo):

    __slots__ = ('name', '_label', '_documentation', 'coordinates')

    def __init__(self,
                 name: str,
                 relevant_coordinates: t.Optional[list[str]] = None):
        self.coordinates = set(relevant_coordinates) if relevant_coordinates is not None else set()
        super().__init__(name)

    def update_coordinates(self, coordinates: ReferenceValue):
        self.coordinates.update(_BaseInfo.build_all_from_ref(coordinates))


class OCProc2ElementInfo(_BaseInfo):

    __slots__ = ('ioos_category', 'essential_ocean_vars', 'name', '_label', '_documentation', 'allow_many',
                 'group_name', 'preferred_unit', 'data_type', 'min_value', 'max_value', 'allowed_values')

    def __init__(self,
                 name: str,
                 allow_multi: bool = True,
                 min_value: t.Optional[float] = None,
                 max_value: t.Optional[float] = None,
                 data_type: t.Optional[str] = None,
                 preferred_unit: t.Optional[str] = None,
                 groups: t.Optional[str] = None,
                 allowed_values: t.Optional[set[t.Union[int, str, float]]] = None,
                 ioos_category: t.Optional[str] = None,
                 essential_ocean_variables: t.Optional[set[str]] = None):
        self.group_name: t.Optional[str] = groups
        self.preferred_unit = preferred_unit
        self.data_type = data_type
        self.allow_many = allow_multi
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values or set()
        self.ioos_category = ioos_category or None
        self.essential_ocean_vars = essential_ocean_variables or set()
        super().__init__(name)

    def set_ioos_category(self, ioos_category: ReferenceValue):
        self.ioos_category = _BaseInfo.build_one_from_ref(ioos_category)

    def update_essential_ocean_vars(self, ocean_var: ReferenceValue):
        self.essential_ocean_vars.update(_BaseInfo.build_all_from_ref(ocean_var))

    def set_preferred_unit(self, unit: LiteralValue):
        self.preferred_unit = _BaseInfo.build_one_from_literal(unit)

    def set_data_type(self, data_type: ReferenceValue):
        self.data_type = _BaseInfo.build_one_from_ref(data_type)

    def set_allowed_group(self, group: LiteralValue):
        self.group_name = str(_BaseInfo.build_one_from_literal(group))

    def update_allowed_values(self, avs: LiteralValue):
        self.allowed_values.update(_BaseInfo.build_all_from_literal(avs))

    def set_allow_multi(self, allow_multi: LiteralValue):
        self.allow_many = str(_BaseInfo.build_one_from_literal(allow_multi)).lower().strip() != 'false'

    def set_min_value(self, min_value: LiteralValue):
        self.min_value = _BaseInfo.build_one_from_literal(min_value)

    def set_max_value(self, max_value: LiteralValue):
        self.max_value = _BaseInfo.build_one_from_literal(max_value)


@injector.injectable_global
class OCProc2Ontology:

    def __init__(self, ontology_file: t.Optional[pathlib.Path] = None):
        self._onto_files = []
        if ontology_file:
            self._onto_files.append(ontology_file)
        else:
            vocab_dir = pathlib.Path(__file__).absolute().parent.parent.parent.parent / 'vocab'
            self._onto_files.append(vocab_dir / 'eov.ttl')
            self._onto_files.append(vocab_dir / 'ioos.ttl')
            self._onto_files.append(vocab_dir / 'cnodc.ttl')
            self._onto_files.append(vocab_dir / 'rstypes.ttl')
        self._parameters: t.Optional[dict[str, OCProc2ElementInfo]] = None
        self._recordset_types: t.Optional[dict[str, OCProc2ChildRecordTypeInfo]] = None
        self._load_lock = threading.Lock()
        self._load_graph()

    def _load_graph(self):
        if self._parameters is None:
            with self._load_lock:
                if self._parameters is None:
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
                    for key in graph_dict:
                        if SKOS_IN_SCHEME not in graph_dict[key]:
                            continue
                        if str(graph_dict[key][SKOS_IN_SCHEME]) == CNODC_ELEMENTS:
                            e_name = key[key.rfind('#')+1:]
                            self._parameters[e_name] = OCProc2ElementInfo(e_name)
                            if SKOS_LABEL in graph_dict[key]:
                                self._parameters[e_name].set_label(graph_dict[key][SKOS_LABEL])
                            if SKOS_DOCUMENTATION in graph_dict[key]:
                                self._parameters[e_name].set_documentation(graph_dict[key][SKOS_DOCUMENTATION])
                            if CNODC_PREF_UNIT in graph_dict[key]:
                                self._parameters[e_name].set_preferred_unit(graph_dict[key][CNODC_PREF_UNIT])
                            if CNODC_DATA_TYPE in graph_dict[key]:
                                self._parameters[e_name].set_data_type(graph_dict[key][CNODC_DATA_TYPE])
                            if CNODC_GROUP in graph_dict[key]:
                                self._parameters[e_name].set_allowed_group(graph_dict[key][CNODC_GROUP])
                            if CNODC_ALLOW_MULTI in graph_dict[key]:
                                self._parameters[e_name].set_allow_multi(graph_dict[key][CNODC_ALLOW_MULTI])
                            if CNODC_MIN in graph_dict[key]:
                                self._parameters[e_name].set_min_value(graph_dict[key][CNODC_MIN])
                            if CNODC_MAX in graph_dict[key]:
                                self._parameters[e_name].set_max_value(graph_dict[key][CNODC_MAX])
                            if CNODC_ALLOW in graph_dict[key]:
                                self._parameters[e_name].update_allowed_values(graph_dict[key][CNODC_ALLOW])
                            if CNODC_EOV in graph_dict[key]:
                                self._parameters[e_name].update_essential_ocean_vars(graph_dict[key][CNODC_EOV])
                            if CNODC_IOOS_CATEGORY in graph_dict[key]:
                                self._parameters[e_name].set_ioos_category(graph_dict[key][CNODC_IOOS_CATEGORY])
                        elif str(graph_dict[key][SKOS_IN_SCHEME]) == CNODC_RECORDSET_TYPES:
                            e_name = key[key.rfind('#')+1:]
                            self._recordset_types[e_name] = OCProc2ChildRecordTypeInfo(key)
                            if SKOS_LABEL in graph_dict[key]:
                                self._recordset_types[e_name].set_label(graph_dict[key][SKOS_LABEL])
                            if SKOS_DOCUMENTATION in graph_dict[key]:
                                self._recordset_types[e_name].set_documentation(graph_dict[key][SKOS_DOCUMENTATION])
                            if CNODC_COORDINATE in graph_dict[key]:
                                self._recordset_types[e_name].update_coordinates(graph_dict[key][CNODC_COORDINATE])

    def recordset_info(self, recordset_type_name: str) -> t.Optional[OCProc2ChildRecordTypeInfo]:
        if recordset_type_name in self._recordset_types:
            return self._recordset_types[recordset_type_name]
        return None

    def recordset_exists(self, recordset_type: str) -> bool:
        return recordset_type in self._recordset_types

    def coordinates(self, recordset_type: str) -> t.Optional[set]:
        if recordset_type in self._recordset_types:
            return self._recordset_types[recordset_type].coordinates or None
        return None

    def info(self, element_name: str) -> t.Optional[OCProc2ElementInfo]:
        if element_name in self._parameters:
            return self._parameters[element_name]
        return None

    def allow_many(self, element_name: str) -> t.Optional[bool]:
        if element_name in self._parameters:
            return self._parameters[element_name].allow_many
        return None

    def preferred_unit(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].preferred_unit
        return None

    def group_name(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].group_name
        return None

    def data_type(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].data_type
        return None

    def exists(self, element_name: str) -> bool:
        return element_name in self._parameters

    def min_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        if element_name in self._parameters:
            return self._parameters[element_name].min_value
        return None

    def max_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        if element_name in self._parameters:
            return self._parameters[element_name].max_value
        return None

    def allowed_values(self, element_name: str) -> t.Optional[set[t.Union[str, int, float]]]:
        if element_name in self._parameters:
            return self._parameters[element_name].allowed_values or None
        return None

    def ioos_category(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].ioos_category
        return None

    def essential_ocean_vars(self, element_name: str) -> t.Optional[set[str]]:
        if element_name in self._parameters:
            return self._parameters[element_name].essential_ocean_vars or None
        return None
