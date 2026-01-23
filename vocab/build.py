import csv
import os
import shutil
import pathlib
import typing as t

DIR = pathlib.Path(__file__).absolute().parent

def map_group(group_human_name: str):
    """ Convert the name of a group in the spreadsheet to the programmatic term. """
    if group_human_name == 'Metadata (Record Only)':
        return 'metadata:record'
    elif group_human_name == 'Metadata (Element Only)':
        return 'metadata:element'
    return group_human_name.lower()

def map_vocab(vocab_name: str):
    """ Convert the name of a vocabulary to the prefix in TTL. """
    if vocab_name == 'BUFR v4':
        return 'bufr4'
    elif vocab_name == 'BODC':
        return 'p01'
    elif vocab_name == 'MEDS PCODE':
        return 'pcode'
    elif vocab_name == 'Ocean Gliders':
        return 'og1'
    return vocab_name

def map_map_type(map_type: str):
    """ Convert the name of a type of match to the SKOS keyword. """
    if map_type == 'Exact':
        return 'exactMatch'
    elif map_type == 'Narrower':
        return 'narrowerMatch'
    elif map_type == 'Broader':
        return 'broaderMatch'
    elif map_type == 'Related':
        return 'relatedMatch'
    return map_type.replace(' ', '')

def map_data_type(dtype: str):
    """ Map a data type word to the TTL element to be used. """
    if dtype == 'datetime':
        return 'xsd:dateTimeStamp'
    elif dtype == 'list':
        return 'cnodc:List'
    elif dtype == 'text':
        return 'xsd:string'
    elif dtype == 'element':
        return 'cnodc:Element'
    elif dtype in ('decimal', 'integer', 'date'):
        return f'xsd:{dtype.lower()}'
    else:
        raise ValueError(f'Invalid data type {dtype}')

def read_lines_csv(file_path, header0: str) -> t.Iterable[tuple]:
    with open(file_path, 'r', encoding='utf-8-sig') as h:
        reader = csv.reader(h)
        for row in reader:
            if not row:
                continue
            if row[0] == header0:
                continue
            yield row


# Load all the mapping files (they're sorted by vocabulary)
maps = {}
for file in os.scandir(DIR / "data" / "mappings"):
    # Only look at CSV files
    if file.name.endswith(".csv"):
        for row in read_lines_csv(file.path, 'Element Short Name'):
            element_name, match_type, vocab_name, code = row
            if element_name not in maps:
                maps[element_name] = {}
            if vocab_name not in maps[element_name]:
                maps[element_name][vocab_name] = []
            maps[element_name][vocab_name].append((match_type, code))


# We'll write to a temporary file to make sure we don't overwrite the good file with one with an error in it.
temp_file = DIR / 'cnodc.ttl.temp'
with open(temp_file, 'w', encoding='utf-8') as output:
    
    # Copy the base turtle file
    with open(DIR / 'data' / 'base.ttl', 'r', encoding='utf-8') as base:
        output.write(base.read())
        
    # Write out all the IOOS categories
    for row in read_lines_csv(DIR / 'data' / 'ioos_categories.csv', 'Short Name'):
        output.write("\n")
        # Concept name
        output.write(f'cnodc:{row[0]} rdf:type skos:Concept ;\n')
        # English & French labels
        output.write(f'  skos:prefLabel "{row[1]}"@en ;\n')
        if row[2]:
            output.write(f'  skos:prefLabel "{row[2]}"@fr ;\n')
        # Additional documentation
        if row[3]:
            output.write(f'  skos:documentation "{row[3]}"@en ;\n')
        if row[4]:
            output.write(f'  skos:documentation "{row[4]}"@fr ;\n')
        # Add it to the scheme
        output.write(f'  skos:inScheme cnodc:ioos_categories .\n')
        
    # Write out all the element names
    for row in read_lines_csv(DIR / 'data' / 'elements.csv', 'Short Name'):
        output.write("\n")
        # Concept name
        output.write(f'cnodc:{row[0]} rdf:type skos:Concept ;\n')
        # Labels
        if row[1]:
            output.write(f'  skos:prefLabel "{row[1]}"@en ;\n')
        if row[2]:
            output.write(f'  skos:prefLabel "{row[2]}"@fr ;\n')
        # Documentation
        if row[3]:
            output.write(f'  skos:documentation "{row[3]}"@en ;\n')
        if row[4]:
            output.write(f'  skos:documentation "{row[4]}"@fr ;\n')
        # Element group
        output.write(f'  cnodc:elementGroup "{map_group(row[5])}" ;\n')
        # Type of data that can be stored in it
        output.write(f'  cnodc:dataType {map_data_type(row[6])} ;\n')
        # Preferred units (any unit that can be converted simply into these units is acceptable)
        if row[7]:
            output.write(f'  cnodc:preferredUnit "{row[7]}" ;\n')
        # CF standard name
        if row[8]:
            output.write(f'  cnodc:standardName "{row[8]}" ;\n')
        # CNODC bilingual variable name
        if row[9]:
            output.write(f'  cnodc:variableName "{row[9]}" ;\n')
        # IOOS category
        if row[10]:
            output.write(f'  cnodc:ioosCategory cnodc:{row[10]} ; \n')
        # Minimum valid value
        if row[11]:
            output.write(f'  cnodc:minValue {row[11]} ; \n')
        # Maximum valid value
        if row[12]:
            output.write(f'  cnodc:maxValue {row[12]} ; \n')
        # Allowed values, separated by semicolons
        if row[13]:
            for allowed_value in row[13].split(';'):
                output.write(f'  cnodc:allowedValue "{allowed_value}" ;\n')
        # Whether to ignore this field when checking for duplicates.
        if row[14] and row[14] == 'Y':
            output.write(f'  cnodc:ignoreInDuplicateCheck "True" ;\n')
        # Whether to allow multiple values for this element.
        if row[15] and row[15] == 'Y':
            output.write(f'  cnodc:allowMulti "True" ;\n')
        # Write out all the mappings.
        if row[0] in maps:
            for vocab_name in maps[row[0]]:
                if vocab_name == 'WMO Codes':
                    for mapping in maps[row[0]][vocab_name]:
                        output.write(f'  cnodc:wmoCodeGroup "{mapping[1]}" ; \n')
                else:
                    prefix = map_vocab(vocab_name)
                    for mapping in maps[row[0]][vocab_name]:
                        if prefix == 'bufr4':
                            actual_prefix = f"{prefix}{mapping[1][0:3]}"
                            code = mapping[1][3:]
                        else:
                            code = mapping[1]
                            actual_prefix = prefix
                        output.write(f'  skos:{map_map_type(mapping[0])} {actual_prefix}:{code} ;\n')
        # The scheme
        output.write('  skos:inScheme cnodc:elements .\n')

    for row in read_lines_csv(DIR / 'data' / 'rs_types.csv', 'Short Name'):
        output.write('\n')
        # element name
        output.write(f'cnodc:{row[0]}  rdf:type skos:Concept ;\n')
        # labels
        if row[1]:
            output.write(f'  skos:prefLabel "{row[1]}"@en ;\n')
        if row[2]:
            output.write(f'  skos:prefLabel "{row[2]}"@fr ;\n')
        # if present, at least one of these coordinates is required for it to make sense.
        if row[3]:
            for coordinate_name in row[3].split(';'):
                output.write(f'  cnodc:requireCoordinate cnodc:{coordinate_name} ;\n')
        # scheme
        output.write(f'  skos:inScheme cnodc:recordSetTypes .\n')

shutil.copy(temp_file, DIR / "cnodc.ttl")
os.remove(temp_file)