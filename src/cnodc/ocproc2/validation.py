import rdflib
import pathlib
import threading
import typing as t
from autoinject import injector


SKOS_IN_SCHEME = 'http://www.w3.org/2004/02/skos/core#inScheme'
CNODC_PREFIX = 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#'
CNODC_ELEMENTS = f'{CNODC_PREFIX}elements'
CNODC_RECORDSET_TYPES = f'{CNODC_PREFIX}recordSetTypes'
CNODC_PREF_UNIT = f'{CNODC_PREFIX}preferredUnit'
CNODC_DATA_TYPE = f'{CNODC_PREFIX}dataType'
CNODC_GROUP = f'{CNODC_PREFIX}elementGroup'
CNODC_ALLOW_MULTI = f'{CNODC_PREFIX}allowMulti'
CNODC_MIN = f'{CNODC_PREFIX}minValue'
CNODC_MAX = f'{CNODC_PREFIX}maxValue'
CNODC_ALLOW = f'{CNODC_PREFIX}allowedValue'


@injector.injectable_global
class OCProc2Ontology:

    def __init__(self, ontology_file: pathlib.Path = None):
        self._onto_file = ontology_file or pathlib.Path(__file__).absolute().parent / 'ontology' / 'parameters.ttl'
        self._parameters = None
        self._recordset_types = set()
        self._load_lock = threading.Lock
        self._load_graph()

    def _load_graph(self):
        if self._parameters is None:
            with self._load_lock:
                if self._parameters is None:
                    self._parameters = {}
                    graph = rdflib.Graph()
                    graph.parse(str(self._onto_file))
                    graph_dict = {}
                    for a, b, c in graph:
                        a = str(a)
                        b = str(b)
                        c = str(c)
                        if a not in graph_dict:
                            graph_dict[a] = {}
                        if b in graph_dict[a]:
                            if isinstance(graph_dict[a][b], str):
                                graph_dict[a][b] = set(graph_dict[a][b])
                            graph_dict[a][b].add(c)
                        else:
                            graph_dict[a][b] = c
                    for key in graph_dict:
                        if not key.startswith(CNODC_PREFIX):
                            continue
                        if SKOS_IN_SCHEME not in graph_dict[key]:
                            continue
                        if graph_dict[key][SKOS_IN_SCHEME] == CNODC_ELEMENTS:
                            e_name = key[key.rfind('#')+1:]
                            self._parameters[e_name] = {
                                'preferred_unit': None,
                                'data_type': None,
                                'groups': set(),
                                'allow_multi': True,
                                'min_value': None,
                                'max_value': None,
                                'allowed_values': set()
                            }
                            if CNODC_PREF_UNIT in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_PREF_UNIT], set):
                                    self._parameters[e_name]['preferred_unit'] = list(graph_dict[key][CNODC_PREF_UNIT])[0]
                                else:
                                    self._parameters[e_name]['preferred_unit'] = graph_dict[key][CNODC_PREF_UNIT]
                            if CNODC_DATA_TYPE in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_DATA_TYPE], set):
                                    data_type = list(graph_dict[key][CNODC_DATA_TYPE])[0]
                                else:
                                    data_type = graph_dict[key][CNODC_DATA_TYPE]
                                self._parameters[e_name]['data_type'] = data_type[data_type.rfind('#')+1:]
                            if CNODC_GROUP in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_GROUP], set):
                                    self._parameters[e_name]['groups'] = graph_dict[key][CNODC_GROUP]
                                else:
                                    self._parameters[e_name]['groups'] = set(graph_dict[key][CNODC_GROUP])
                            if CNODC_ALLOW_MULTI in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_ALLOW_MULTI], set):
                                    self._parameters[e_name]['allow_multi'] = not any(graph_dict[key][CNODC_ALLOW_MULTI] == 'false')
                                else:
                                    self._parameters[e_name]['allow_multi'] = graph_dict[key][CNODC_ALLOW_MULTI] != 'false'
                            if CNODC_MIN in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_MIN], set):
                                    self._parameters[e_name]['min_value'] = list(graph_dict[key][CNODC_MIN])[0]
                                else:
                                    self._parameters[e_name]['min_value'] = graph_dict[key][CNODC_MIN]
                            if CNODC_MAX in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_MAX], set):
                                    self._parameters[e_name]['max_value'] = list(graph_dict[key][CNODC_MAX])[0]
                                else:
                                    self._parameters[e_name]['min_value'] = graph_dict[key][CNODC_MAX]
                            if CNODC_ALLOW in graph_dict[key]:
                                if isinstance(graph_dict[key][CNODC_ALLOW], set):
                                    self._parameters[e_name]['allowed_values'] = graph_dict[key][CNODC_ALLOW]
                                else:
                                    self._parameters[e_name]['allowed_values'] = set(graph_dict[key][CNODC_ALLOW])

                        elif graph_dict[key][SKOS_IN_SCHEME] == CNODC_RECORDSET_TYPES:
                            self._recordset_types.add(key[key.rfind('#')+1:])

    def allow_multiple_values(self, element_name: str) -> bool:
        if element_name in self._parameters:
            return self._parameters[element_name]['allow_multi']
        return True

    def preferred_unit(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name]['preferred_unit']
        return None

    def element_group(self, element_name: str) -> t.Optional[set[str]]:
        if element_name in self._parameters:
            return self._parameters[element_name]['groups']
        return None

    def data_type(self, element_name: str) -> t.Optional[str]:
        if element_name in self._parameters:
            return self._parameters[element_name]['data_type']
        return None

    def is_defined_element(self, element_name: str) -> bool:
        return element_name in self._parameters

    def is_defined_recordset_type(self, recordset_type: str) -> bool:
        return recordset_type in self._recordset_types

    def min_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        return self._parameters[element_name]['min_value']

    def max_value(self, element_name: str) -> t.Optional[t.Union[float, int]]:
        return self._parameters[element_name]['max_value']

    def allowed_values(self, element_name: str) -> set[t.Union[str, int]]:
        return self._parameters[element_name]['allowed_values']
