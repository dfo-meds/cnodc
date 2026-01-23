This directory contains the element codes for the OCPROC2 vocabulary.

# OCPROC2 Context
OCPROC2 stores the data and metadata associated with a single observation (called a record). A record is usually either
a set of measurements taken at a specific time and place or a profile of measurements taken at various depths but at a 
specific time and horizontal place. 

Within the OCPROC2, each recorded value or piece of metadata is called an "element" which is identified by a unique
code. These files document and describe those codes.

# Elements


# Mappings
These are stored under the mappings directory, and outline how OCPROC2 elements are related to other commonly used 
vocabularies.

0: The name of an element, as it exists in elements.csv column 0
1: The match type:
     Narrower: The term listed in this row is more specific than the one referenced in column 0
     Broader: The term listed in this row is less specific
     Exact: The term listed in this row is exactly the same
     Related: The term listed in this row is related to the one in column 0 but not exactly the same, nor more or less broad.
2: The vocabulary the term is from. New vocabularies need to be added in base.ttl and to the map_vocab() function above.
     BUFR v4: BUFR identifiers from Table B of the BUFRv4 codes (see https://github.com/wmo-im/BUFR4/)
     MEDS PCODE: MEDS parameter codes from OCPROC or MEDS ASCII formats
     BODC: The British Oceanographic Data Centre Parameter Usage Vocabulary (see https://vocab.nerc.ac.uk/collection/P01/current/)
     Ocean Gliders: The OceanGliders Parameter Usage Vocabulary (see https://vocab.nerc.ac.uk/collection/OG1/current/)
     WMO Codes: While not related to a specific vocabulary, this documents where the element can be found in the WMO ASCII Code Forms
3. The actual term from the vocabulary (i.e. the ID from the NERC server or the actual PCODE / ASCII code element)


# IOOS Categories

# Record Set (RS) Types
