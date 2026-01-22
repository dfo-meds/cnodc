import csv
import os
import shutil

maps = {}

with open('./code_mappings.csv', 'r', encoding='utf-8-sig') as code_maps:
    for line in code_maps.readlines():
        line = line.strip()
        if line == '':
            continue
        if line.startswith('Element Short Name,'):
            continue
        pieces = line.split(',')
        if pieces[0] not in maps:
            maps[pieces[0]] = {}
        if pieces[2] not in maps[pieces[0]]:
            maps[pieces[0]][pieces[2]] = []
        maps[pieces[0]][pieces[2]].append((pieces[1], pieces[3]))

def map_group(group_human_name: str):
    if group_human_name == 'Metadata (Record Only)':
        return 'metadata:record'
    elif group_human_name == 'Metadata (Element Only)':
        return 'metadata:element'
    return group_human_name.lower()

def map_vocab(vocab_name: str):
    if vocab_name == 'BUFR v4':
        return 'bufr4'
    elif vocab_name == 'CF Standard Names':
        return 'p07'
    elif vocab_name == 'BODC':
        return 'p01'
    elif vocab_name == 'MEDS PCODE':
        return 'pcode'
    return vocab_name

def map_map_type(map_type: str):
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


with open('./parameters.ttl', 'w', encoding='utf-8') as output:
    with open('./base.ttl', 'r', encoding='utf-8') as base:
        output.write(base.read())
    with open('./ioos_categories.csv', 'r', encoding='utf-8-sig') as ioos:
        for line in ioos.readlines():
            line = line.strip()
            if line == '':
                continue
            if line[0] == '#':
                continue
            if line.startswith('Short Name,'):
                continue
            pieces = line.split(',')
            output.write("\n")
            output.write(f'cnodc:{pieces[0]} rdf:type skos:Concept ;\n')
            output.write(f'  skos:prefLabel "{pieces[1]}"@en ;\n')
            if len(pieces) > 2 and pieces[2]:
                output.write(f'  skos:prefLabel "{pieces[2]}"@fr ;\n')
            if len(pieces) > 3 and pieces[3]:
                output.write(f'  skos:documentation "{pieces[3]}"@en ;\n')
            if len(pieces) > 4 and pieces[4]:
                output.write(f'  skos:documentation "{pieces[4]}"@fr ;\n')
            output.write(f'  skos:inScheme cnodc:ioos_categories .\n')
    with open('./elements.csv', 'r', encoding='utf-8-sig') as elements:
        reader = csv.reader(elements)
        for line in reader:
            if not line:
                continue
            if line[0] == 'Short Name' or line[0] == '':
                continue
            output.write("\n")
            output.write(f'cnodc:{line[0]} rdf:type skos:Concept ;\n')
            if line[1]:
                output.write(f'  skos:prefLabel "{line[1]}"@en ;\n')
            if line[2]:
                output.write(f'  skos:prefLabel "{line[2]}"@fr ;\n')
            if line[3]:
                output.write(f'  skos:documentation "{line[3]}"@en ;\n')
            if line[4]:
                output.write(f'  skos:documentation "{line[4]}"@fr ;\n')
            output.write(f'  cnodc:elementGroup "{map_group(line[5])}" ;\n')
            output.write(f'  cnodc:dataType {map_data_type(line[6])} ;\n')
            if line[7]:
                output.write(f'  cnodc:preferredUnit "{line[7]}" ;\n')
            if line[8]:
                output.write(f'  cnodc:standardName "{line[8]}" ;\n')
            if line[9]:
                output.write(f'  cnodc:variableName "{line[9]}" ;\n')
            if line[10]:
                output.write(f'  cnodc:ioosCategory cnodc:{line[10]} ; \n')
            if line[11]:
                output.write(f'  cnodc:minValue {line[11]} ; \n')
            if line[12]:
                output.write(f'  cnodc:maxValue {line[12]} ; \n')
            if line[13]:
                for allowed_value in line[13].split(';'):
                    output.write(f'  cnodc:allowedValue "{allowed_value}" ;\n')
            if line[14] and line[14] == 'Y':
                output.write(f'  cnodc:ignoreInDuplicateCheck "True" ;\n')
            if line[15] and line[15] == 'Y':
                output.write(f'  cnodc:allowMulti "True" ;\n')
            if line[0] in maps:
                for vocab_name in maps[line[0]]:
                    if vocab_name == 'WMO Codes':
                        for mapping in maps[line[0]][vocab_name]:
                            output.write(f'  cnodc:wmoCodeGroup "{mapping[1]}" ; \n')
                    else:
                        prefix = map_vocab(vocab_name)
                        for mapping in maps[line[0]][vocab_name]:
                            if prefix == 'bufr4':
                                actual_prefix = f"{prefix}{mapping[1][0:3]}"
                                code = mapping[1][3:]
                            else:
                                code = mapping[1]
                                actual_prefix = prefix
                            output.write(f'  skos:{map_map_type(mapping[0])} {actual_prefix}:{code} ;\n')




            output.write('  skos:inScheme cnodc:elements .\n')

    with open('./rs_types.csv', 'r', encoding='utf-8') as rs_types:
        for line in rs_types.readlines():
            line = line.strip()
            if line == '':
                continue
            if line.startswith('Short Name,'):
                continue
            pieces = line.split(',')
            output.write('\n')
            output.write(f'cnodc:{pieces[0]}  rdf:type skos:Concept ;\n')
            if pieces[1]:
                output.write(f'  skos:prefLabel "{pieces[1]}"@en ;\n')
            if pieces[2]:
                output.write(f'  skos:prefLabel "{pieces[2]}"@fr ;\n')
            if pieces[3]:
                for coordinate_name in pieces[3].split(';'):
                    output.write(f'  cnodc:requireCoordinate cnodc:{coordinate_name} ;\n')
            output.write(f'  skos:inScheme cnodc:recordSetTypes .\n')

shutil.copy("./parameters.ttl", "../ontology/parameters.ttl")
os.remove("./parameters.ttl")