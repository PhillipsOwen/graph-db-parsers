import os
import argparse
import json
import csv
from codetiming import Timer

"""
Methods to parse ORION data and create an import file to load into a MemGraph DB.

    example node input record
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

    example edge input record
    {
      "subject":"UNII:7PK6VC94OU","predicate":"biolink:affects","object":"NCBIGene:6531",
      "primary_knowledge_source":"infores:ctd","description":"decreases activity of",
      "NCBITaxon":"9606","publications":["PMID:30776375"],"knowledge_level":"knowledge_assertion",
      "agent_type":"manual_agent","object_aspect_qualifier":"activity","object_direction_qualifier":"decreased",
      "qualified_predicate":"biolink:causes"
    }        
        
note: the procedure to create/import data is to: 
  - create the json file
    mg_build_individual_json.py
    --node-infile=nodes-orig.jsonl
    --edge-infile=edges-orig.jsonl
    --data-dir=D:/dvols/graph-eval/ctd_data
    --outfile=ctd-nodes.json
    --type=node
    
  - copy the json file to the memgraph pod
    k -n translator-exp --retries=10 cp merge.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/merges.json

  - execute the following command in the memgraph UI 
    CALL import_util.json("/var/lib/memgraph/databases/memgraph/merge.json");

  - confirm data loaded properly
    see cypher-cmds.txt for more commands

 CSV: --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=D:/dvols/graph-eval/ctd_data 
--outfile=nodes.json --type=csv

Nodes: --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=D:/dvols/graph-eval/ctd_data 
--outfile=nodes.json --type=node

Edges: --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=D:/dvols/graph-eval/ctd_data
--outfile=edges.json --type=edge      
    
to make json pretty:
    cat <created json file>.json | jq >> <created json file>-pretty.json && head -40 <created json file>-pretty.json
    
to remove 2 chars at the beginning of the file:
    cut -c 3- /mnt/d/dvols/graph-eval/ctd-merge-all-lines-file0.json > output.txt
    printf '%s%s' "[" "$(cat output.txt)" > /mnt/d/dvols/graph-eval/ctd-merge-all-lines-file0.json
    
to view beginning and ending of a file:
    head -c 50 <file name>
    tail -c 50 <file name>
    
robokop data stats:    
    (venv-3.10) [powen@compute-5-2 ~]$ python3 mg_build_merge_json.py --node-infile rk-nodes.jsonl --edge-infile rk-edges.jsonl 
    --data-dir graph-eval --outfile robokop

    Parsing all input edge(s) with associated nodes per file into as many files as it takes.

    Parsing data for output file 1...
        Nodes parsed in 159.983s
        Edges parsed in 2451.547s

    Final stats: 9835893 node(s) and 137418343 edge(s) processed.
"""


def process_csv_file(_data_dir, _infile, _outfile):
    """
    deprecated: this method parses the ORION input files and converts them to CSV.

    deprecated: this was abandoned in favor of using JSON files.

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    # the base to the target directory
    _test_dir: str = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # init the data storage
        out_data: list = []

        # spin through the data nad get the column names
        field_names: set = get_csv_field_names(_data_dir, _infile)

        # for each line in the file
        for line in in_file:
            # get the data into a JSON object
            d_line = json.loads(line)

            # loop through the keys
            for key in field_names:
                # if the item is in the data
                if key in d_line:
                    # get the first item in a list. this is not opportune and full array parsing could not be achieved
                    d_line[key]: str = str(d_line[key][0]) if isinstance(d_line[key], list) else str(d_line[key])
                else:
                    d_line[key]: str = str('')

            # save the JSON object in the array
            out_data.append(d_line)

        with open(os.path.join(_test_dir, _outfile), 'w', encoding='utf-8') as out_file:
            # open the output file
            writer = csv.DictWriter(out_file, fieldnames=field_names)

            # write out the data
            writer.writeheader()
            writer.writerows(out_data)


def get_csv_field_names(_data_dir, _infile) -> set:
    """
    deprecated: loops through the ORION input file and returns a set of field names for the CSV header record.

    :param _data_dir:
    :param _infile:
    :return:
    """
    ret_val: set = set()

    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # get the keys
        for line in in_file:
            # load the data
            d_line = json.loads(line)

            # save the keys
            ret_val = ret_val | d_line.keys()

    # return the keys
    return ret_val


def process_edge_file(_data_dir, _infile, _outfile):
    """
    process the edge file.

    this method creates import data that uses json_util.load_from_path() to load a single JSON file.

    after processing copy up to the MemGraph server pod
        k -n translator-exp --retries=10 cp edges.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/edges.json
    """

    # open the data files
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir,  _outfile), 'w', encoding='utf-8') as out_file):

        # init the local variables
        d_line: dict = {}
        edge_data: list = []

        # define the way the edges are remapped fpr memgraph
        edge_key_map: dict = {'subject': 'start', 'object': 'end', 'predicate': 'label'}

        # init the edge counter
        final_edge_count: int = 0

        # setup a timer
        with Timer(name="edges", text="\tEdges parsed in {:.3f}s"):
            # for each line in the file
            for line in in_file:
                # load the JSON item
                d_line = json.loads(line)

                """
                target MemGraph edge record contains:
                  {
                      "id": <>,
                      "start": <>,
                      "end": <>,
                      "label": <>,
                      "properties": {},
                      "type": "relationship"
                  },
    
                """

                # init the base record info
                base: dict = {"type": "relationship", "id": final_edge_count}

                # remap the data
                out_record: dict = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

                # save all attributes in the property element
                properties: dict = {"properties": {k: v for k, v in d_line.items() if k not in edge_key_map.keys()}}

                # save the remapped data
                out_record.update(base)
                out_record.update(properties)

                # save the updated item and remove non-ascii chars
                edge_data.append(json.dumps(out_record, ensure_ascii=True))

                # increment the edge counters
                final_edge_count += 1

            # save the data as a string
            out_str = ",".join(edge_data)

            # make the data look like an array
            out_file.write('[' + out_str + ']')

            print('\nFinal Edge stats: {final_edge_count} node(s)'.format(final_edge_count=final_edge_count))


def process_node_file(_data_dir, _infile, _outfile):
    """
        process the node file.

        this method creates import data that uses json_util.load_from_path() to load a single JSON file.

        after processing copy up to the MemGraph server pod
            k -n translator-exp --retries=10 cp nodes.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/nodes.json
    """
    # open the data files
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        # init the local variables
        d_line: dict = {}
        node_data: list = []

        # init node counter
        total_node_count: int = 0

        # setup a timer
        with Timer(name="nodes", text="\tNodes parsed in {:.3f}s"):
            # for each line in the file
            for line in in_file:
                """
                node input data contains:
                
                {
                    "id":"UNII:7PK6VC94OU",
                    "name":"4-Methylaminorex",
                    "category":["biolink:SmallMolecule","biolink:MolecularEntity","biolink:ChemicalEntity","biolink:PhysicalEssence","biolink:ChemicalOrDrugOrTreatment","biolink:ChemicalEntityOrGeneOrGeneProduct","biolink:ChemicalEntityOrProteinOrPolypeptide","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],
                    "equivalent_identifiers":["UNII:7PK6VC94OU","PUBCHEM.COMPOUND:92196","DRUGBANK:DB01447","MESH:C000081","CAS:3568-94-3","HMDB:HMDB0246502","INCHIKEY:LJQBMYDFWFGESC-UHFFFAOYSA-N","UMLS:C0097249"]}
                """

                # load the JSON item
                d_line = json.loads(line)

                # save the updated item and remove non-ascii chars
                node_data.append(json.dumps(d_line, ensure_ascii=True))

                # increment the total number of nodes counter
                total_node_count += 1

            # save the data as a string
            out_str = ",".join(node_data)

            # make the data look like an array
            out_file.write('[' + out_str + ']')

        print('\nFinal Node stats: {final_node_count} node(s)'.format(final_node_count=total_node_count))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--csv-infile', dest='csv_infile', type=str, help='CSV input file')
    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--type', dest='type', type=str, help='run type')

    args = parser.parse_args()

    run_type: str = args.type.upper()
    print(f'Processing for a {run_type} run type...')

    # process the node file
    if run_type == 'NODE':
        process_node_file(args.data_dir, args.node_infile, args.outfile)

    # process the edge file
    elif run_type == 'EDGE':
        process_edge_file(args.data_dir, args.edge_infile, args.outfile)
    else:
        print('Unknown or missing processing type.')

    # deprecated: process the csv file
    # elif run_type == 'CSV':
    #     process_csv_file(args.data_dir, args.csv_infile, args.outfile)
