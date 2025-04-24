import os
import argparse
import json
import csv

"""
Methods to parse ORION data and create an import file to load into a MemGraph DB.

note: the procedure to create/import data is to: 
  - create the json file
    mg-convert.py
    --node-infile=nodes-orig.jsonl 
    --edge-infile=edges-orig.jsonl 
    --data-dir=C:/Users/powen/PycharmProjects/translator-graph/data 
    --outfile=memgraph-out.json --type=merge
    
  - copy the json file to the memgraph pod
    k -n translator-exp --retries=10 cp merge.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/merges.json

  - execute the following command in the memgraph UI 
    CALL import_util.json("/var/lib/memgraph/databases/memgraph/merge.json");

  - confirm data loaded properly
    see cypher-cmds.txt for more commands

deprecated: --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=C:/Users/powen/PycharmProjects/translator-graph/data 
--outfile=nodes.json --type=node

deprecated: --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=C:/Users/powen/PycharmProjects/translator-graph/data 
--outfile=edges.json --type=edge      
"""


def process_csv_file(_data_dir, _infile, _outfile):
    """
    this method parses the ORION input files and converts them to CSV.

    deprecated: this was abandoned in favor of using JSON files.

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    # the base to this directory
    _test_dir = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # init the data storage
        data = []

        # spin through the data nad get the column names
        field_names = get_field_names(_data_dir, _infile)

        # for each line in the file
        for line in in_file:
            # get the data into a JSON object
            d_line = json.loads(line)

            # loop through the keys
            for key in field_names:
                # if the item is in the data
                if key in d_line:
                    # get the first item in a list. this is not opportune and full array parsing could not be achieved
                    d_line[key] = str(d_line[key][0]) if isinstance(d_line[key], list) else str(d_line[key])
                else:
                    d_line[key] = str('')

            # save the JSON object in the array
            data.append(d_line)

        with open(os.path.join(_test_dir, _outfile), 'w', encoding='utf-8') as out_file:
            # open the output file
            writer = csv.DictWriter(out_file, fieldnames=field_names)

            # write out the data
            writer.writeheader()
            writer.writerows(data)


def get_field_names(_data_dir, _infile):
    """
    deprecated: loops through the ORION input file and returns a set of field names.

    :param _data_dir:
    :param _infile:
    :return:
    """
    ret_val = set()

    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        for line in in_file:  # get the keys
            d_line = json.loads(line)
            ret_val = ret_val | d_line.keys()

    return ret_val


def process_edge_file(_data_dir, _infile, _outfile):
    """
    process the edge file.

    deprecated: this was abandoned in favor of using import_util.json() to load JSON files.

    after processing copy up to the MemGraph server pod
        k -n translator-exp --retries=10 cp edges.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/edges.json
    """

    # open the data files
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir,  _outfile), 'w', encoding='utf-8') as out_file):

        # init the local variables
        d_line = {}
        out_data = []
        line_counter = 0
        edge_key_map = {'subject': 'start', 'object': 'end', 'predicate': 'label'}

        # for each line in the file
        for line in in_file:
            # load the JSON item
            d_line = json.loads(line)

            # target MemGraph record contains:
            #   {
            #       "id": <>,
            #       "start": <>,
            #       "end": <>,
            #       "label": <>,
            #       "properties": {},
            #       "type": "relationship"
            #   },

            # d_line input record
            #   {
            #       "subject":"UNII:7PK6VC94OU","predicate":"biolink:affects","object":"NCBIGene:6531",
            #       "primary_knowledge_source":"infores:ctd","description":"decreases activity of",
            #       "NCBITaxon":"9606","publications":["PMID:30776375"],"knowledge_level":"knowledge_assertion",
            #       "agent_type":"manual_agent","object_aspect_qualifier":"activity","object_direction_qualifier":"decreased",
            #       "qualified_predicate":"biolink:causes"
            #   }

            # # remap the data for a memgraph edge
            # d_line["id"] = line_counter
            # d_line["start"] = d_line['subject']
            # d_line["end"] = d_line['object']
            # d_line["label"] = d_line['predicate']
            # d_line["type"] = "relationship"
            # d_line["properties"] = json.dumps({"primary_knowledge_source": d_line["primary_knowledge_source"]})
            #
            # # save the updated item. remove non-ascii chars
            # out_data.append(json.dumps(d_line, ensure_ascii=True))
            #
            # line_counter += 1

            # init the output record
            out_record = {"type": "relationship", "id": line_counter}

            # remap the data
            out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

            # save all attributes in the property element
            properties = {"properties": {k: v for k, v in d_line.items() if k not in edge_key_map.keys()}}

            # save the remapped data
            out_record.update(properties)

            # save the data in the output array
            out_data.append(json.dumps(out_record, ensure_ascii=True))

            line_counter += 1

            # if line_counter > 1:
            #     break

        # save the data as a string
        out_str = ", ".join(out_data)

        # make the data look like an array
        out_file.write('[' + out_str + ']')


def process_node_file(_data_dir, _infile, _outfile):
    """
        process the node file.

        deprecated: this was abandoned in favor of using import_util.json() to load JSON files.

        after processing copy up to the MemGraph server pod
            k -n translator-exp --retries=10 cp nodes.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/nodes.json
    """
    # open the data files
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        # init the local variables
        d_line = {}
        out_data = []

        # for each line in the file
        for line in in_file:
            # load the JSON item
            d_line = json.loads(line)

            # save the updated item. remove non-ascii chars
            out_data.append(json.dumps(d_line, ensure_ascii=True))

        # save the data as a string
        out_str = ", ".join(out_data)

        # make the data look like an array
        out_file.write('[' + out_str + ']')


def merge_nodes_edges(_data_dir, _node_infile, _edge_infile, _outfile):
    """
    Creates a data file that has memgraph nodes and edges.

    note this data must be loaded with the memgraph import_data.json() call

    after processing copy up to the MemGraph server pod
        k -n translator-exp --retries=10 cp nodes.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/merge.json

    :param _data_dir:
    :param _node_infile:
    :param _edge_infile:
    :param _outfile:
    :return:
    """
    # open the data files
    with (open(os.path.join(_data_dir, _node_infile), 'r', encoding='utf-8') as in_node_file,
          open(os.path.join(_data_dir, _edge_infile), 'r', encoding='utf-8') as in_edge_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        # init the local variables
        out_record = {}
        out_data = []
        line_counter = 0

        """
        {
            "id": 6114,  <---- "id" from nodes file goes into this "id" field
            "labels": [
                "Person" <---- "category" list from nodes file goes into this "labels" field
            ],
            "properties": {
                "name": "Anna" <---- all other properties here
            },
            "type": "node"
        },

        node input record
        {
            "id":"UNII:7PK6VC94OU",
            "name":"4-Methylaminorex",
            "category":
            [
                "biolink:SmallMolecule","biolink:MolecularEntity","biolink:ChemicalEntity",
                "biolink:PhysicalEssence","biolink:ChemicalOrDrugOrTreatment","biolink:ChemicalEntityOrGeneOrGeneProduct",
                "biolink:ChemicalEntityOrProteinOrPolypeptide","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"
            ],
            "equivalent_identifiers":
            [
                "UNII:7PK6VC94OU",
                "PUBCHEM.COMPOUND:92196",
                "DRUGBANK:DB01447",
                "MESH:C000081",
                "CAS:3568-94-3",
                "HMDB:HMDB0246502",
                "INCHIKEY:LJQBMYDFWFGESC-UHFFFAOYSA-N","UMLS:C0097249"
            ]
        }
        """

        # create a map for the node data
        node_key_map = {'id': 'id', 'category': 'labels'}

        # for each line in the file
        for line in in_node_file:
            # load the JSON item
            d_line = json.loads(line)

            # remap the data
            out_record = {node_key_map.get(k, k): v for k, v in d_line.items() if k in node_key_map.keys()}

            # save all attributes
            attributes = {"type": "node", "properties": {k: v for k, v in d_line.items()}}

            # save the remapped data
            out_record.update(attributes)

            # save the data in the output array
            out_data.append(json.dumps(out_record, ensure_ascii=True))

            # line_counter += 1
            #
            # if line_counter > 10:
            #     break

        """
        {
            "end": 6116,  <----- "object" from edges file goes here
            "id": 21121,
            "label": "IS_FRIENDS_WITH",  <---- "predicate" here
            "properties": {},  <--- all other properties here
            "start": 6114,  <----- "subject" from edges file goes here
            "type": "relationship"
        },

        edge input record
        {
            "subject":"UNII:7PK6VC94OU",
            "predicate":"biolink:affects",
            "object":"NCBIGene:6531",
            "primary_knowledge_source":"infores:ctd",
            "description":"decreases activity of",
            "NCBITaxon":"9606",
            "publications":
            [
                "PMID:30776375"
            ],
            "knowledge_level":"knowledge_assertion",
            "agent_type":"manual_agent",
            "object_aspect_qualifier":"activity",
            "object_direction_qualifier":"decreased",
            "qualified_predicate":"biolink:causes"
        }
        """

        # create a map for the edge data
        edge_key_map = {'subject': 'start', 'object': 'end', 'predicate': 'label'}

        # for each line in the file
        for line in in_edge_file:
            # load the JSON item
            d_line = json.loads(line)

            # remap the data
            out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

            # save all attributes
            attributes = {"type": "relationship", "id": line_counter, "properties": {k: v for k, v in d_line.items()}}

            # save the remapped data
            out_record.update(attributes)

            # save the data in the output array
            out_data.append(json.dumps(out_record, ensure_ascii=True))

            line_counter += 1

            # if line_counter > 10:
            #     break

        # convert to text
        out_str = ", ".join(out_data)

        # write the output file
        out_file.write('[' + out_str + ']')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--csv-infile', dest='csv_infile', type=str, help='CSV input file')
    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--type', dest='type', type=str, help='run type')
    args = parser.parse_args()

    run_type = args.type.upper()
    print(f'Processing a {run_type} run type...')

    # process the csv file
    if run_type == 'CSV':
        process_csv_file(args.data_dir, args.csv_infile, args.outfile)

    # process the node file
    elif run_type == 'NODE':
        process_node_file(args.data_dir, args.node_infile, args.outfile)

    # process the edge file
    elif run_type == 'EDGE':
        process_edge_file(args.data_dir, args.edge_infile, args.outfile)

    # process the node and edge files
    elif run_type == 'MERGE':
        merge_nodes_edges(args.data_dir, args.node_infile, args.edge_infile, args.outfile)

    else:
        print('Unknown or missing processing type.')
