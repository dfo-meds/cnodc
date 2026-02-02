import rdflib
import pathlib
import threading
import typing as t
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


class _BaseInfo:

    __slots__ = ('name', '_label', '_documentation')

    def __init__(self, name):
        self.name = name
        self._label = {}
        self._documentation = {}

    def label(self, lang: str = 'en') -> str:
        return self._get_language_attribute(self._label, lang, self.name)

    def documentation(self, lang: str = 'en') -> str:
        return self._get_language_attribute(self._documentation, lang)

    def _get_language_attribute(self, lang_attr: dict, lang: str = 'en', default: str = '') -> str:
        if lang in lang_attr and lang_attr[lang] != '':
            return lang_attr[lang]
        elif 'und' in lang_attr and lang_attr['und'] != '':
            return lang_attr['und']
        elif 'en' in self._label and lang_attr['en'] != '':
            return lang_attr['en']
        else:
            return default

    def _set_multi_language(self, label, property: dict):
        lang = getattr(label, 'language') if hasattr(label, 'language') else 'und'
        if lang is None or lang == '':
            lang = 'und'
        property[lang] = str(label)

    def set_label(self, label: t.Union[set, str]):
        if isinstance(label, set):
            for l in label:
                self._set_multi_language(l, self._label)
        else:
            self._set_multi_language(label, self._label)

    def set_documentation(self, doc: t.Union[set, str]):
        if isinstance(doc, set):
            for d in doc:
                self._set_multi_language(d, self._documentation)
        else:
            self._set_multi_language(doc, self._documentation)

    def _remove_prefix(self, x: t.Union[set, str]):
        if x is None:
            return x
        if isinstance(x, set):
            return set(self._remove_prefix(y) for y in x)
        else:
            return x if '#' not in x else x[x.rfind('#')+1:]


class OCProc2ChildRecordTypeInfo(_BaseInfo):

    __slots__ = ('name', '_label', '_documentation', 'coordinates')

    def __init__(self,
                 name: str,
                 relevant_coordinates: t.Optional[list[str]] = None):
        self.coordinates = relevant_coordinates or set()
        super().__init__(name)

    def update_coordinates(self, coordinates: t.Union[set, str]):
        if isinstance(coordinates, set):
            self.coordinates.update(self._remove_prefix(coordinates))
        else:
            self.coordinates.add(self._remove_prefix(coordinates))


class OCProc2ElementInfo(_BaseInfo):

    __slots__ = ('ioos_category', 'essential_ocean_vars', 'name', '_label', '_documentation', 'allow_multi', 'groups', 'preferred_unit', 'data_type', 'min_value', 'max_value', 'allowed_values')

    def __init__(self,
                 name: str,
                 allow_multi: bool = True,
                 min_value: t.Optional[float] = None,
                 max_value: t.Optional[float] = None,
                 data_type: t.Optional[str] = None,
                 preferred_unit: t.Optional[str] = None,
                 groups: t.Optional[set[str]] = None,
                 allowed_values: t.Optional[set[t.Union[int, str]]] = None,
                 ioos_category: t.Optional[str] = None,
                 essential_ocean_variables: t.Optional[set[str]] = None):
        self.groups = groups or set()
        self.preferred_unit = preferred_unit
        self.data_type = data_type
        self.allow_multi = allow_multi
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values or set()
        self.ioos_category = ioos_category or None
        self.essential_ocean_vars = essential_ocean_variables or set()
        super().__init__(name)

    def set_ioos_category(self, ioos_category: t.Union[set, str]):
        if isinstance(ioos_category, set):
            ioos_category = list(ioos_category)[0]
        self.ioos_category = self._remove_prefix(ioos_category)

    def update_essential_ocean_vars(self, ocean_var: t.Union[str, set]):
        if isinstance(ocean_var, set):
            self.essential_ocean_vars.update(self._remove_prefix(ocean_var))
        else:
            self.essential_ocean_vars.add(self._remove_prefix(ocean_var))

    def set_preferred_unit(self, unit: t.Union[set, str]):
        if isinstance(unit, set):
            self.preferred_unit = list(unit)[0]
        elif unit is None or unit == '':
            self.preferred_unit = None
        else:
            self.preferred_unit = unit

    def set_data_type(self, data_type: t.Union[set, str]):
        if isinstance(data_type, set):
            dt = list(data_type)[0]
        elif data_type is None or data_type == '':
            dt = None
        else:
            dt = data_type
        self.data_type = dt[dt.rfind('#')+1:] if dt else None

    def update_groups(self, groups: t.Union[set, str]):
        if isinstance(groups, set):
            self.groups.update(groups)
        else:
            self.groups.add(groups)

    def update_allowed_values(self, avs: t.Union[set, str, int]):
        if isinstance(avs, set):
            self.allowed_values.update(avs)
        else:
            self.allowed_values.add(avs)

    def set_allow_multi(self, allow_multi: t.Union[set, str]):
        if isinstance(allow_multi, set):
            self.allow_multi = not any(str(x).lower() == 'false' for x in allow_multi)
        else:
            self.allow_multi = str(allow_multi).lower() != 'false'

    def set_min_value(self, min_value: t.Union[set, float]):
        if isinstance(min_value, set):
            self.min_value = float(list(min_value)[0])
        else:
            self.min_value = float(min_value)

    def set_max_value(self, max_value: t.Union[set, float]):
        if isinstance(max_value, set):
            self.max_value = float(list(max_value)[0])
        else:
            self.max_value = float(max_value)


@injector.injectable_global
class OCProc2Ontology:

    def __init__(self, ontology_file: pathlib.Path = None):
        self._onto_file = ontology_file or pathlib.Path(__file__).absolute().parent.parent.parent.parent / 'vocab' / 'cnodc.ttl'
        self._parameters: t.Optional[dict[str, OCProc2ElementInfo]] = None
        self._recordset_types: t.Optional[dict, str, OCProc2ChildRecordTypeInfo] = None
        self._load_lock = threading.Lock()
        self._load_graph()

    def _load_graph(self):
        if self._parameters is None:
            with self._load_lock:
                if self._parameters is None:
                    self._parameters = {}
                    self._recordset_types = {}
                    graph = rdflib.Graph()
                    graph.parse(str(self._onto_file))
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
                        if not key.startswith(CNODC_PREFIX):
                            continue
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
                                self._parameters[e_name].update_groups(graph_dict[key][CNODC_GROUP])
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

    def element_info(self, element_name: str) -> t.Optional[OCProc2ElementInfo]:
        if element_name in self._parameters:
            return self._parameters[element_name]
        return None

    def allow_multiple_values(self, element_name: str) -> bool:
        if element_name in self._parameters:
            return self._parameters[element_name].allow_multi
        return True

    def preferred_unit(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].preferred_unit
        return None

    def element_group(self, element_name: str) -> t.Optional[set[str]]:
        if element_name in self._parameters:
            return self._parameters[element_name].groups
        return None

    def data_type(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name].data_type
        return None

    def is_defined_element(self, element_name: str) -> bool:
        return element_name in self._parameters

    def is_defined_recordset_type(self, recordset_type: str) -> bool:
        return recordset_type in self._recordset_types

    def min_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        if element_name in self._parameters:
            return self._parameters[element_name].min_value
        return None

    def max_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        if element_name in self._parameters:
            return self._parameters[element_name].max_value
        return None

    def allowed_values(self, element_name: str) -> t.Optional[set[t.Union[str, int]]]:
        if element_name in self._parameters:
            return self._parameters[element_name].allowed_values
        return None
