import os
import argparse
import json
import csv
import re
from codetiming import Timer
from common.logger import LoggingUtil

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("mg_build_individual_json", level=log_level, line_format='medium', log_file_path=log_path)


"""
Methods to parse ORION data and create an import file to load into a MemGraph DB.

    example node input record (CTD)
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

    example edge input record (CTD)
    {
      "subject":"UNII:7PK6VC94OU",
      "predicate":"biolink:affects",
      "object":"NCBIGene:6531",
      "primary_knowledge_source":"infores:ctd",
      "description":"decreases activity of",
      "NCBITaxon":"9606",
      "publications":["PMID:30776375"],
      "knowledge_level":"knowledge_assertion",
      "agent_type":"manual_agent",
      "object_aspect_qualifier":"activity",
      "object_direction_qualifier":"decreased",
      "qualified_predicate":"biolink:causes"
    }        
        
  - execute the following command in the memgraph UI 
    CALL import_util.json("/omnicorp/graph-eval-<input file name>");

  - confirm data loaded properly
    see cypher-cmds.txt for more commands
    
to make json pretty:
    cat <created json file>.json | jq >> <created json file>-pretty.json && head -40 <created json file>-pretty.json
    
to remove 2 chars at the beginning of the file:
    cut -c 3- /mnt/d/dvols/graph-eval/ctd-merge-all-lines-file0.json > output.txt
    printf '%s%s' "[" "$(cat output.txt)" > /mnt/d/dvols/graph-eval/ctd-merge-all-lines-file0.json
    
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


def process_csv_header(_data_dir, _infile) -> str:
    """
    parses the robokop csv header line and creates a Memgraph LOAD CSV script

    note that this expects the header from the nodes.temp_csv and edges.temp.csv file.

    the load csv command for nodes is:
    -------------------------------------

    load csv from "/var/log/memgraph/ctd-nodes.csv" with header as row
    create (n: Node
        {
            <col_name>: <optional to some type>(row.<csv_col_name>),
            ...
        })
    with n
        match (n: Node)
        set n: n.category;

    the load csv command for edges is:
    -------------------------------------

    load csv from "/var/log/memgraph/ctd-edges.csv" with header as row
    with row
      match (a: Node {id: row.subject}), (b: Node {id: row.object})
      create (a)-
        [e: row.predicate
          {
            <col_name>: <optional: to some type>(row.<csv_col_name>),
            ...
          }
        ]->(b);

    :param _data_dir:
    :param _infile:
    :return:
    """
    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # load a file iterator for the edges and nodes
        file_iter: iter = iter(in_file)

        array_split_char = ';'

        ret_val: str = ''
        csv_hdr: list = []

        try:
            # for each line in the file
            while True:
                # get a line of data
                cols = next(file_iter)

                # split the line by the delimiter character
                cols = cols.split('\t')

                # go through each column header element (<name>:<type>)
                for col in cols:
                    val = get_conversion(col, array_split_char)
                    ret_val += val

                    csv_hdr.append(val.strip().split(':')[0])

                # only process the first header line
                raise StopIteration

        except StopIteration:
            logger.debug('Number of columns in %s, to process: %s', os.path.join(_data_dir, _infile), len(cols))

            print(','.join(csv_hdr))

            # finish up the header
            ret_val = '{\n' + ret_val[:-2] + '\n}'
            print(ret_val)

        return ret_val


def get_conversion(column_name: str, array_split_char: str) -> str:
    """
    Note that the input data is from the original ORION column header. this data is in the format <data name>:<data type> and
    is used to convert the data in the memgraph import query.

    :param column_name:
    :param array_split_char:
    :return:
    """
    col_items = column_name.strip().split(':')

    target_col_name = ":".join(col_items[0: -1])

    target_col_name = re.sub(r'[^A-Za-z0-9_]', '_', target_col_name)

    ret_val: str = f'{target_col_name}: '

    match col_items[-1]:
        case 'END_ID' | 'START_ID'| 'TYPE' | 'string' | 'ID':
            ret_val += f'row.{target_col_name}'
        case 'boolean':
            ret_val += f'toBoolean(row.{target_col_name})'
        case 'float':
            ret_val += f'toFloat(row.{target_col_name})'
        case 'float[]':
            ret_val += f"split(CASE WHEN row.{target_col_name} <> '' AND row.{target_col_name} IS NOT NULL THEN row.{target_col_name} ELSE '' END, ';') AS stringList UNWIND stringList AS str WITH toFloat(str) AS floatValue RETURN COLLECT(floatValue)"
        case 'int':
            ret_val += f'toInteger(row.{target_col_name})'
        case 'string[]' | 'LABEL':
            ret_val += f"split(row.{target_col_name}, '{array_split_char}')"
        case _:
            ret_val = f"ERROR: No data type recognised for {column_name}"

    return '\t' + ret_val + ',\n'


def process_edge_file(_data_dir, _infile, _outfile, _max_items):
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
        first_record = True

        # define the way the edges are renamed for memgraph
        edge_key_map: dict = {'subject': 'start', 'object': 'end', 'predicate': 'label'}

        # init the edge counter
        total_edge_count: int = 0

        # load a file iterator for the edges and nodes
        edge_file_iter: iter = iter(in_file)

        # start the output
        out_file.write('[')

        logger.debug('Parsing data for edge output file...')

        # setup a timer
        with Timer(name="edges", text="\tEdges parsed in {:.3f}s"):
            try:
                # for each line in the file
                while True:
                    # get a line of data
                    line = next(edge_file_iter)

                    # load the JSON item
                    d_line = json.loads(line)

                    # remap the data
                    out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

                    # save all property attributes, id and the record type
                    attributes = {"type": "relationship", "id": total_edge_count, "properties": {k: v for k, v in d_line.items()}}

                    # save the remapped data
                    out_record.update(attributes)

                    # first time in no leading comma
                    if first_record:
                        # save the data in the output array and default non-ascii characters
                        out_file.write(json.dumps(out_record, ensure_ascii=True))
                        first_record = False
                    else:
                        # save the data in the output array and default non-ascii characters
                        out_file.write(',' + json.dumps(out_record, ensure_ascii=True))

                    # increment the edge counter
                    total_edge_count += 1

                    # testing only, terminate if this amount has been reached
                    if total_edge_count == _max_items:
                        # end the file
                        out_file.write(']')
                        break

            except StopIteration:
                # end the file
                out_file.write(']')

                # flush the output file data to disk
                out_file.flush()

            logger.debug('Final Edge stats: %s edge(s): ', total_edge_count)


def process_node_file(_data_dir, _infile, _outfile, _max_items):
    """
        process the node file.

        this method creates import data that uses json_util.load_from_path() to load a single JSON file.
    """
    # open the data files
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        # start the output
        out_file.write('[')

        # init the local variables
        d_line: dict = {}
        first_record = True

        # define the way the nodes are renamed for memgraph
        node_key_map: dict = {'id': 'id', 'category': 'labels'}

        # load a file iterator for the edges and nodes
        node_file_iter: iter = iter(in_file)

        # init node counter
        total_node_count: int = 0

        with Timer(name="nodes", text="\tNodes parsed in {:.3f}s"):
            try:
                # until we reach the desired number of lines processed
                while True:
                    # get a line of data
                    line = next(node_file_iter)

                    # load the JSON item
                    d_line = json.loads(line)

                    # remap the data
                    out_record = {node_key_map.get(k, k): v for k, v in d_line.items() if k in node_key_map.keys()}

                    # save all properties and the record type
                    attributes = {"type": "node", "properties": {k: v for k, v in d_line.items()}}

                    # save the remapped data
                    out_record.update(attributes)

                    # first time in no leading comma
                    if first_record:
                        # save the data in the output array and default non-ascii characters
                        out_file.write(json.dumps(out_record, ensure_ascii=True))

                        first_record = False
                    else:
                        # save the data in the output array and default non-ascii characters
                        out_file.write(',' + json.dumps(out_record, ensure_ascii=True))

                    # increment the node counter
                    total_node_count += 1

                    # testing only, terminate if this amount has been reached
                    if total_node_count == _max_items:
                        # end the file
                        out_file.write(']')
                        break

            except StopIteration:
                out_file.write(']')

                # flush the output file data to disk
                out_file.flush()

        logger.debug('Final Node stats:%s node(s)', total_node_count)

# def create_csv_header():


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--csv-infile', dest='csv_infile', type=str, help='CSV input file')
    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--max-items', dest='max_items', type=str, help='Output file')
    parser.add_argument('--type', dest='type', type=str, help='run type')

    args = parser.parse_args()

    run_type: str = args.type.upper()
    logger.debug(f'Processing for a %s run type.', run_type)

    # process the node file
    if run_type == 'NODE':
        process_node_file(args.data_dir, args.node_infile, args.outfile, args.max_items)

    # process the edge file
    elif run_type == 'EDGE':
        process_edge_file(args.data_dir, args.edge_infile, args.outfile, args.max_items)

    elif run_type == 'COLHDR':
        node_hdr = process_csv_header(args.data_dir, args.node_infile)
        edge_hdr = process_csv_header(args.data_dir, args.edge_infile)

    # elif run_type == 'CREATECSVHDR':
    #     node_hdr = create_csv_header(args.data_dir, args.node_infile)
    #     edge_hdr = create_csv_header(args.data_dir, args.edge_infile)
    else:
        logger.error('Unknown or missing processing type.')

    # deprecated: process the csv file
    # elif run_type == 'CSV':
    #     process_csv_file(args.data_dir, args.csv_infile, args.outfile)
