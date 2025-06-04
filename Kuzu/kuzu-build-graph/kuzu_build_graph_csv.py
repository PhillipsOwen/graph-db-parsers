import os
import shutil
import argparse
import kuzu
import re
import csv
from codetiming import Timer
from common.logger import LoggingUtil
import pandas as pd

"""
this code takes the node/edge csv files and parses them into a Kuzu DB

powen, 2025-04-22 
"""

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("kuzu_build_graph_csv", level=log_level, line_format='medium', log_file_path=log_path)

# the location of this file
test_dir = os.path.dirname(os.path.abspath(__file__))


def create_kuzu_tables(conn: kuzu.Connection, _data_dir, _node_file, _edge_file) -> None:
    """
    creates the node and edge tables in kuzu

    :param _edge_file:
    :param _node_file:
    :param _data_dir:
    :param conn:
    :return:
    """

    n_cols: str = process_csv_header(_data_dir, _node_file)

    conn.execute(f"CREATE NODE TABLE rkNode({n_cols}, PRIMARY KEY (id))")

    e_cols = process_csv_header(_data_dir, _edge_file)

    conn.execute(f"CREATE REL TABLE rkEdge(FROM rkNode TO rkNode, {e_cols})")


def process_csv_header(_data_dir, _infile):
    """
    parses the robokop csv header line and creates

    note that this expects the header from the nodes.temp_csv and edges.temp.csv file.

    :param _data_dir:
    :param _infile:
    :return:
    """
    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # load a file iterator for the edges and nodes
        file_iter: iter = iter(in_file)

        array_split_char = ';'

        ret_val: str = ''

        try:
            # for each line in the file
            while True:
                # get a line of data
                cols = next(file_iter)

                cols = cols.split('\t')

                logger.debug('Number of columns in %s to process: %s', os.path.join(_data_dir, _infile), len(cols))

                for col in cols:
                    ret_val += get_conversion(col, array_split_char)

                # stop processing after the first (header) line
                raise StopIteration

        except StopIteration:
            ret_val = ret_val[:-1]
            return ret_val


def get_conversion(column_name: str, array_split_char: str) -> str:
    """
    Note that the input data is from the original ORION column header. this data is in the format <data name>:<data type> and
    is used to convert the data in the memgraph import query.

    :param column_name:
    :param array_split_char:
    :return:
    """
    # if column_name.find('.') > 0:
    #     return ''

    col_items = column_name.strip().split(':')

    ret_val: str = ''  # '//' if col_items[0].find('.') > 0 else ''

    target_col_name = ":".join(col_items[0: -1])

    target_col_name = re.sub(r'[^A-Za-z0-9_]', '_', target_col_name)

    match col_items[-1]:
        case 'START_ID' | 'TYPE' | 'END_ID' | 'string' | 'ID':
            ret_val += f"{target_col_name} STRING"
        case 'boolean':
            ret_val += f"{target_col_name} BOOLEAN"
        case 'float':
            ret_val += f"{target_col_name} FLOAT"
        case 'int':
            ret_val += f"{target_col_name} INT32"
            # print(target_col_name, 'INT32')
        case 'float[]':
            ret_val += f"{target_col_name} FLOAT[]"
            # print(target_col_name, 'FLOAT[]')
        case 'string[]' | 'LABEL':
            ret_val += f"{target_col_name} STRING[]"
            # print(target_col_name, 'STRING[]')
        case _:
            ret_val = f"ERROR: No data type recognised for {column_name}"

    # print(ret_val)
    return ret_val + ','


def parse_data(conn: kuzu.Connection, _data_dir, _node_infile, _edge_infile) -> None:
    """
    parses/loads the node/edge JSON data into a Kuzu DB

    :param conn:
    :param _data_dir:
    :param _node_infile:
    :param _edge_infile:
    :return:
    """
    node_count = 0
    edge_count = 0

    with Timer(name="nodes", text="DB nodes loaded in {:.2f}s"):
        logger.debug("Loading nodes into the database...")

        for i in range(1, 21):
            inf = os.path.join(_data_dir, _node_infile + str(i) + '.csv')
            inf = str(inf).replace('\\', '/')

            logger.debug("Loading node file %s into the database...", inf)

            conn.execute(f'COPY rkNode FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=false);')

            logger.debug(f"Loaded node file %s into the DB.", inf)

    with Timer(name="edges", text="DB edges loaded in {:.2f}s"):
        logger.debug("Loading edges into the database...")

        for i in range(1, 24):
            inf = os.path.join(_data_dir, _edge_infile + str(i) + '.csv')
            inf = str(inf).replace('\\', '/')

            logger.debug("Loading edge file %s into the database...", inf)

            conn.execute(f'COPY rkEdge FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=false);')

            logger.debug(f'Loaded edge file %s into the DB.', inf)

    logger.debug(f"Successfully loaded nodes and edges into the DB.")


def convert_file(_data_dir, _infile, file_type):
    with Timer(name="files", text="DB files converted in {:.2f}s"):
        logger.debug(f"Converting {file_type} files...")

        if file_type == 'NODE':
            rng = range(1, 21)
        elif file_type == 'EDGE':
            rng = range(1, 24)

        for i in rng:
            inf = os.path.join(_data_dir, _infile + str(i) + '.csv')

            # so this works in both a windows and linux environment
            inf = str(inf).replace('\\', '/')

            out_file = os.path.join(_data_dir, _infile + 'conv' + str(i) + '.csv')

            # so this works in both a windows and linux environment
            out_file = str(out_file).replace('\\', '/')

            logger.debug("Converting file %s into %s", inf, out_file)

            # load the infile into a pandas object
            df = pd.read_csv(inf, low_memory=False)

            # convert the list columns to be Kuzu compatible. e.g: "[]", [1,2,3,...], [1.2,2.3,3.4,...], [txt1,txt2, ...]
            if file_type == 'NODE':
                # define the target array list columns
                reformat_list_cols: list = ['category', 'equivalent_identifiers', 'hgvs']

                # define the target int32 columns
                reformat_int32_cols: list = ['lipinski', 'arom_c', 'sp3_c', 'sp2_c', 'sp_c', 'halogen', 'hetero_sp2_c', 'rotb', 'o_n', 'oh_nh', 'rgb',
                                             'fda_labels']

            elif file_type == 'EDGE':
                # define the target array list columns
                reformat_list_cols: list = ['p_value', 'supporting_affinities', 'slope', 'publications', 'hetio_source', 'tmkp_ids', 'expressed_in',
                                       'pubchem_assay_ids', 'patent_ids', 'aggregator_knowledge_source', 'category', 'provided_by', 'has_evidence',
                                       'qualifiers', 'phosphorylation_sites', 'drugmechdb_path_id', 'complex_context']

                # define the target int32 columns
                reformat_int32_cols: list = ['distance_to_feature']

                # duplicate the subject and object columns into from and to columns.
                # it is a Kuzu requirement that the first 2 columns be from and to.
                df.insert(loc=0, column='to', value=df['object'])
                df.insert(loc=0, column='from', value=df['subject'])

            # apply the new formatting for lists
            for col in reformat_list_cols:
                df[col] = df[col].apply(lambda x: [] if pd.isna(x) else '[' + ','.join(map(str, x.split(';'))) + ']')

            # apply the new formatting for INT32 data
            for col in reformat_int32_cols:
                df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(int(x)))

            # create the new file
            df.to_csv(out_file, index=False)

            logger.debug(f"%s file %s converted and exported to %s.", file_type, inf, out_file)


if __name__ == "__main__":
    """
    command line:
    
    python3 kuzu_build_graph_csv.py --node-infile=rk-orig-node-cols.temp_csv --edge-infile=rk-orig-edge-cols.temp_csv --data-dir=D:/dvols/graph-eval/robokop_data/kuzu --outfile=rk-kuzu-db --type=tables
    
    fastapi pod command line:
    cd /logs
           
    python3 --node-infile=rk-nodes-pt20.csv --edge-infile=rk-edges-pt23.csv --data-dir=D:/dvols/graph-eval/robokop_data/kuzu --outfile=rk-kuzu-db --type=data
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--type', dest='type', type=str, help='Data operation type (tables or data)')

    args = parser.parse_args()

    run_type: str = args.type.upper()

    # get the path to the DB
    db_dir: str = os.path.join(args.data_dir, str(args.outfile))

    # # wipe the DB if we are creating tables
    if run_type == "TABLES" and os.path.isdir(db_dir):
        # Delete directory each time until we have MERGE FROM available in kuzu
        shutil.rmtree(db_dir, ignore_errors=True)

    # Create the database
    db = kuzu.Database(db_dir, max_db_size=274877906944)

    # get a DB connection
    connection = kuzu.Connection(db)

    try:
        # create the tables if requested
        if run_type == "TABLES":
            # create new node and edge tables
            create_kuzu_tables(connection, args.data_dir, args.node_infile, args.edge_infile)

        # parse the data if requested
        if run_type == "DATA":
            # parse the data
            parse_data(connection, args.data_dir, args.node_infile, args.edge_infile)

        if run_type == "CONVERT":
            convert_file(args.data_dir, args.node_infile, 'NODE')
            convert_file(args.data_dir, args.edge_infile, 'EDGE')

    except Exception as e:
        logger.exception(f'Exception parsing')
    finally:
        connection.close()
