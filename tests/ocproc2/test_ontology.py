from cnodc.ocproc2 import OCProc2Ontology, OCProc2ElementInfo, OCProc2ChildRecordTypeInfo
from helpers.base_test_case import BaseTestCase


def _build_ontology(temp_dir, content: str):
    file = temp_dir / 'test.ttl'
    with open(file, 'w') as h:
        h.write("""
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix cnodc: <http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/cnodc.ttl#> .
@prefix ioos: <http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/ioos.ttl#> .
@prefix eov: <http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/eov.ttl#> .
@prefix rstypes: <http://cnodc-cndoc.dfo-mpo.gc.ca/ocproc2/rstypes.ttl#> .
@prefix p01: <http://vocab.nerc.ac.uk/collection/P01/current/> .
@prefix p07: <http://vocab.nerc.ac.uk/collection/P07/current/> .
@prefix p09: <http://vocab.nerc.ac.uk/collection/P09/current/> .
@prefix og1: <https://vocab.nerc.ac.uk/collection/OG1/current/> .
@prefix bufr4B01: <https://codes.wmo.int/bufr4/b/01/> .
@prefix bufr4B02: <https://codes.wmo.int/bufr4/b/02/> .
@prefix bufr4B03: <https://codes.wmo.int/bufr4/b/03/> .
@prefix bufr4B04: <https://codes.wmo.int/bufr4/b/04/> .
@prefix bufr4B05: <https://codes.wmo.int/bufr4/b/05/> .
@prefix bufr4B06: <https://codes.wmo.int/bufr4/b/06/> .
@prefix bufr4B07: <https://codes.wmo.int/bufr4/b/07/> .
@prefix bufr4B08: <https://codes.wmo.int/bufr4/b/08/> .
@prefix bufr4B09: <https://codes.wmo.int/bufr4/b/09/> .
@prefix bufr4B10: <https://codes.wmo.int/bufr4/b/10/> .
@prefix bufr4B11: <https://codes.wmo.int/bufr4/b/11/> .
@prefix bufr4B12: <https://codes.wmo.int/bufr4/b/12/> .
@prefix bufr4B13: <https://codes.wmo.int/bufr4/b/13/> .
@prefix bufr4B14: <https://codes.wmo.int/bufr4/b/14/> .
@prefix bufr4B20: <https://codes.wmo.int/bufr4/b/20/> .
@prefix bufr4B22: <https://codes.wmo.int/bufr4/b/22/> .
@prefix bufr4B25: <https://codes.wmo.int/bufr4/b/25/> .
@prefix bufr4B33: <https://codes.wmo.int/bufr4/b/33/> .
@prefix bufr4B42: <https://codes.wmo.int/bufr4/b/42/> .
@prefix pcode: <https://www.isdm.gc.ca/isdm-gdsi/diction/code_search-eng.asp?code=> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

cnodc:dataType rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range rdfs:Resource .

cnodc:allowMulti rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:minValue rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:maxValue rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:ignoreInDuplicateCheck rdf:type rdf:Property ;
  rdfs:domain skos:Cocnept ;
  rdfs:range skos:Literal .

cnodc:allowedValue rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:preferredUnit rdf:type rdf:Property ;
  rdfs:subClassOf cnodc:acceptsUnit .

cnodc:elementGroup rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:wmoCodeGroup rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:standardName rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:variableName rdf:type rdf:Property ;
  rdfs:domain skos:Concept ;
  rdfs:range skos:Literal .

cnodc:List rdf:type skos:Concept ;
  skos:prefLabel "List"@en ;
  skos:documentation "List of values" .

cnodc:Element rdf:type skos:Concept ;
    skos:prefLabel "Element"@en ;
    skos:documentation "Another element" .

cnodc:ioosCategory rdf:type rdf:Property ;
    rdfs:domain skos:Concept ;
    rdfs:range ioos:ioosCategories .

cnodc:essentialOceanVariable rdf:type rdf:Property ;
    rdfs:domain skos:Concept ;
    rdfs:range eov:essentialOceanVariables .

cnodc:elements rdf:type skos:ConceptScheme .
            """)
        h.write(content)
    return OCProc2Ontology(file)

class TestBasicOntology(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.ontology = _build_ontology(self.temp_dir, """
cnodc:Parameter rdf:type skos:Concept ;
  skos:prefLabel "Parameter"@en ;
  skos:prefLabel "Parameter but French"@fr ;
  skos:documentation "Documentation"@en ;
  skos:documentation "Documentation but French"@fr ;
  cnodc:ioosCategory ioos:Time ;
  cnodc:essentialOceanVariable eov:seaSurfaceSalinity ;
  cnodc:elementGroup "parameters" ;
  cnodc:preferredUnit "m" ;
  cnodc:dataType xsd:decimal ;
  cnodc:minValue 5.0 ;
  cnodc:maxValue 10.0 ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter3 rdf:type skos:Concept ;
  cnodc:dataType xsd:string ;
  skos:prefLabel "Parameter3"@en ;
  cnodc:elementGroup "metadata" ;
  cnodc:essentialOceanVariable eov:seaSurfaceSalinity ;
  cnodc:essentialOceanVariable eov:subSurfaceSalinity ;
  cnodc:allowedValue "one" ;
  cnodc:allowedValue "two" ;
  cnodc:allowedValue "three" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter4 rdf:type skos:Concept ;
  skos:prefLabel "Parameter4"@fr ;
  cnodc:dataType xsd:dateTimeStamp ;
  cnodc:elementGroup "coordinates" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter5 rdf:type skos:Concept ;
  skos:prefLabel "Parameter5"@und ;
  cnodc:dataType xsd:integer ;
  cnodc:minValue 5 ;
  cnodc:elementGroup "metadata:platform" ;
  cnodc:allowMulti "True" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter6 rdf:type skos:Concept ;
  cnodc:dataType cnodc:List ;
  cnodc:elementGroup "metadata:element" ;
  cnodc:allowMulti "TRUE" ;
  skos:inScheme cnodc:elements .  
  
cnodc:Parameter7 rdf:type skos:Concept ;
  cnodc:dataType xsd:date ;
  cnodc:elementGroup "metadata:mission" ;
  cnodc:allowMulti "true" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter8 rdf:type skos:Concept ;
  cnodc:dataType cnodc:Element ;
  cnodc:elementGroup "metadata:parent" ;
  cnodc:allowMulti "FALSE" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter9 rdf:type skos:Concept ;
  skos:inScheme cnodc:elementsNo .
  
cnodc:Parameter10 rdf:type skos:Concept ;
  cnodc:dataType xsd:integer ;
  cnodc:elementGroup "metadata:product" ;
  cnodc:allowedValue 2 ;
  cnodc:allowedValue 4 ;
  cnodc:allowedValue 6 ;
  cnodc:allowedValue 8 ;
  cnodc:maxValue 10 ;
  cnodc:allowMulti "False" ;
  skos:inScheme cnodc:elements .
  
cnodc:Parameter11 rdf:type skos:Concept ;
  cnodc:dataType xsd:integer ;
  cnodc:elementGroup "metadata:record" ;
  cnodc:allowMulti "false" ;
  skos:inScheme cnodc:elements .
  
ioos:Parameter12 rdf:type skos:Concept ;
  cnodc:dataType xsd:string ;
  cnodc:allowedValue "one" ;
  skos:inScheme cnodc:elements .
  
rstypes:recordSetTypes rdf:type skos:ConceptScheme .

rstypes:requireCoordinate rdf:type rdf:Property ;
  rdfs:domain rstypes:recordSetTypes ;
  rdfs:range cnodc:elements .

rstypes:Type1  rdf:type skos:Concept ;
  skos:prefLabel "hello"@en ;
  skos:documentation "hello2"@en ;
  rstypes:requireCoordinate cnodc:Parameter1 ;
  rstypes:requireCoordinate cnodc:Parameter3 ;
  skos:inScheme rstypes:recordSetTypes .
  
rstypes:Type2  rdf:type skos:Concept ;
  rstypes:requireCoordinate cnodc:Parameter1 ;
  skos:inScheme rstypes:recordSetTypes .

rstypes:Type3  rdf:type skos:Concept ;
  skos:inScheme rstypes:recordSetTypes .
  
        """)

    TEST_INFO = [
        ('Parameter',   True,   "m",    "decimal",          5.0,    10.0,   None,                       "parameters",           "Time", {'seaSurfaceSalinity'},                         True),
        ('Parameter2',  False,),
        ('Parameter3',  True,   None,   "string",           None,   None,   {"one", "two", "three"},    "metadata",             None,   {'subSurfaceSalinity', 'seaSurfaceSalinity'},   True),
        ('Parameter4',  True,   None,   "dateTimeStamp",    None,   None,   None,                       "coordinates",          None,   None,                                           True),
        ('Parameter5',  True,   None,   "integer",          5,      None,   None,                       "metadata:platform",    None,   None,                                           True),
        ('Parameter6',  True,   None,   "List",             None,   None,   None,                       "metadata:element",     None,   None,                                           True),
        ('Parameter7',  True,   None,   "date",             None,   None,   None,                       "metadata:mission",     None,   None,                                           True),
        ('Parameter8',  True,   None,   "Element",          None,   None,   None,                       "metadata:parent",      None,   None,                                           False),
        ('Parameter9',  False,),
        ('Parameter10', True,   None,   "integer",          None,   10,     {2, 4, 6, 8},               "metadata:product",     None,   None,                                           False),
        ('Parameter11', True,   None,   "integer",          None,   None,   None,                       "metadata:record",      None,   None,                                           False),
        ('Parameter12', True,   None,   "string",           None,   None,   {"one"},                    None,                   None,   None,                                           True),
    ]

    def test_ontology_is_defined(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                self.assertIs(info[1], self.ontology.exists(info[0]))

    def test_element_info(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1]:
                    self.assertIsInstance(self.ontology.info(info[0]), OCProc2ElementInfo)
                else:
                    self.assertIsNone(self.ontology.info(info[0]))

    def test_label_both(self):
        self.assertEqual(self.ontology.info("Parameter").label('en'), 'Parameter')
        self.assertEqual(self.ontology.info("Parameter").label('fr'), 'Parameter but French')

    def test_label_english_only(self):
        self.assertEqual(self.ontology.info("Parameter3").label('en'), 'Parameter3')
        self.assertEqual(self.ontology.info("Parameter3").label('fr'), '')

    def test_label_french_only(self):
        self.assertEqual(self.ontology.info("Parameter4").label('fr'), 'Parameter4')
        self.assertEqual(self.ontology.info("Parameter4").label('en'), '')

    def test_label_undefined(self):
        self.assertEqual(self.ontology.info("Parameter5").label('en'), 'Parameter5')
        self.assertEqual(self.ontology.info("Parameter5").label('fr'), 'Parameter5')

    def test_label_omitted(self):
        self.assertEqual(self.ontology.info("Parameter6").label('en'), '')
        self.assertEqual(self.ontology.info("Parameter6").label('fr'), '')

    def test_documentation(self):
        self.assertEqual(self.ontology.info("Parameter").documentation('en'), 'Documentation')
        self.assertEqual(self.ontology.info("Parameter").documentation('fr'), 'Documentation but French')

    def test_preferred_unit(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[2]:
                    self.assertEqual(info[2], self.ontology.info(info[0]).preferred_unit)
                    self.assertEqual(info[2], self.ontology.preferred_unit(info[0]))
                else:
                    self.assertIsNone(self.ontology.preferred_unit(info[0]))

    def test_data_type(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[3]:
                    self.assertEqual(info[3], self.ontology.info(info[0]).data_type)
                    self.assertEqual(info[3], self.ontology.data_type(info[0]))
                else:
                    self.assertIsNone(self.ontology.data_type(info[0]))

    def test_min_value(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[4] is not None:
                    self.assertEqual(info[4], self.ontology.info(info[0]).min_value)
                    self.assertEqual(info[4], self.ontology.min_value(info[0]))
                else:
                    self.assertIsNone(self.ontology.min_value(info[0]))

    def test_max_value(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[5] is not None:
                    self.assertEqual(info[5], self.ontology.info(info[0]).max_value)
                    self.assertEqual(info[5], self.ontology.max_value(info[0]))
                else:
                    self.assertIsNone(self.ontology.max_value(info[0]))

    def test_allowed_values(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[6] is not None:
                    self.assertEqual(info[6], self.ontology.info(info[0]).allowed_values)
                    self.assertEqual(info[6], self.ontology.allowed_values(info[0]))
                else:
                    self.assertIsNone(self.ontology.allowed_values(info[0]))

    def test_allowed_groups(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[7] is not None:
                    self.assertEqual(info[7], self.ontology.info(info[0]).group_name)
                    self.assertEqual(info[7], self.ontology.group_name(info[0]))
                else:
                    self.assertIsNone(self.ontology.group_name(info[0]))

    def test_ioos_category(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[8] is not None:
                    self.assertEqual(info[8], self.ontology.info(info[0]).ioos_category)
                    self.assertEqual(info[8], self.ontology.ioos_category(info[0]))
                else:
                    self.assertIsNone(self.ontology.ioos_category(info[0]))

    def test_eovs(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[9] is not None:
                    self.assertEqual(info[9], self.ontology.info(info[0]).essential_ocean_vars)
                    self.assertEqual(info[9], self.ontology.essential_ocean_vars(info[0]))
                else:
                    self.assertIsNone(self.ontology.essential_ocean_vars(info[0]))

    def test_allow_multiple_values(self):
        for info in TestBasicOntology.TEST_INFO:
            with self.subTest(parameter=info[0]):
                if info[1] and info[10]:
                    self.assertTrue(self.ontology.allow_many(info[0]))
                    self.assertTrue(self.ontology.info(info[0]).allow_many)
                elif info[1] and not info[10]:
                    self.assertFalse(self.ontology.allow_many(info[0]))
                    self.assertFalse(self.ontology.info(info[0]).allow_many)
                else:
                    self.assertIsNone(self.ontology.allow_many(info[0]))

    def test_two_coordinate_recordset(self):
        self.assertTrue(self.ontology.recordset_exists('Type1'))
        self.assertIsInstance(self.ontology.recordset_info('Type1'), OCProc2ChildRecordTypeInfo)
        self.assertEqual(self.ontology.recordset_info('Type1').label('en'), 'hello')
        self.assertEqual(self.ontology.recordset_info('Type1').documentation('en'), 'hello2')
        self.assertEqual(self.ontology.recordset_info('Type1').coordinates, {'Parameter1', 'Parameter3'})
        self.assertEqual(self.ontology.coordinates('Type1'), {'Parameter1', 'Parameter3'})

    def test_one_coordinate_recordset(self):
        self.assertTrue(self.ontology.recordset_exists('Type2'))
        self.assertIsInstance(self.ontology.recordset_info('Type2'), OCProc2ChildRecordTypeInfo)
        self.assertEqual(self.ontology.recordset_info('Type2').coordinates, {'Parameter1'})
        self.assertEqual(self.ontology.coordinates('Type2'), {'Parameter1'})

    def test_no_coordinate_recordset(self):
        self.assertTrue(self.ontology.recordset_exists('Type3'))
        self.assertIsInstance(self.ontology.recordset_info('Type3'), OCProc2ChildRecordTypeInfo)
        self.assertEqual(len(self.ontology.recordset_info('Type3').coordinates), 0)
        self.assertIsNone(self.ontology.coordinates('Type3'))

    def test_bad_recordset(self):
        self.assertFalse(self.ontology.recordset_exists('Type4'))
        self.assertIsNone(self.ontology.recordset_info('Type4'))
        self.assertIsNone(self.ontology.coordinates('Type4'))


class TestActualOntology(BaseTestCase):

    def test_ontology_loads(self):
        ont = OCProc2Ontology()
        self.assertTrue(ont.exists('Temperature'))
        self.assertTrue(ont.recordset_exists('PROFILE'))