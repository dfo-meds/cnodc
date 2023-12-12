import rdflib
import pathlib
import threading
import typing as t
from autoinject import injector


@injector.injectable_global
class OCProc2Ontology:

    def __init__(self, ontology_file: pathlib.Path = None):
        self._onto_file = ontology_file or pathlib.Path(__file__).absolute().parent / 'ontology' / 'parameters.ttl'
        self._parameters = None
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
                        if not key.startswith('http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#'):
                            continue
                        if 'http://www.w3.org/2004/02/skos/core#inScheme' not in graph_dict[key]:
                            continue
                        if graph_dict[key]['http://www.w3.org/2004/02/skos/core#inScheme'] == 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#parameters':
                            parameter_name = key[key.rfind('#')+1:]
                            self._parameters[parameter_name] = {
                                'preferred_unit': None,
                                'data_type': None
                            }
                            if 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#preferredUnit' in graph_dict[key]:
                                self._parameters[parameter_name]['preferred_unit'] = graph_dict[key]['http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#preferredUnit']
                            if 'http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#dataType' in graph_dict[key]:
                                data_type = graph_dict[key]['http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2#dataType']
                                self._parameters[parameter_name]['data_type'] = data_type[data_type.rfind('#')+1:]

    def preferred_unit(self, parameter_name: str) -> t.Optional[str]:
        if parameter_name in self._parameters:
            return self._parameters[parameter_name]['preferred_unit']
        return None

    def data_type(self, parameter_name: str) -> t.Optional[str]:
        if parameter_name in self._parameters:
            return self._parameters[parameter_name]['data_type']
        return None

    def is_defined_parameter(self, parameter_name: str) -> bool:
        return parameter_name in self._parameters
