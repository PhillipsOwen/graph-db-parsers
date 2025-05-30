import os
import shutil
import argparse
import kuzu
import re
import csv
from codetiming import Timer
from common.logger import LoggingUtil

"""
this code takes the node/edge csv files and parses them into a Kuzu DB
using JSON import libraries

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

    cols: str = process_csv_header(_data_dir, _node_file)

    conn.execute(f"CREATE NODE TABLE Node({cols}, PRIMARY KEY (id))")

    cols = process_csv_header(_data_dir, _edge_file)

    conn.execute(f"CREATE REL TABLE Edge(FROM Node TO Node, {cols})")


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
        case 'END_ID' | 'START_ID'| 'TYPE' | 'string' | 'ID':
            ret_val += f"{target_col_name} STRING"
        case 'boolean':
            ret_val += f"{target_col_name} STRING"  # BOOLEAN
        case 'float':
            ret_val += f"{target_col_name} STRING"  # FLOAT
        case 'float[]':
            ret_val += f"{target_col_name} STRING"  # FLOAT[]
        case 'int':
            ret_val += f"{target_col_name} STRING"  # INT32
        case 'string[]' | 'LABEL':
            ret_val += f"{target_col_name} STRING"  # STRING[]
        case _:
            ret_val = f"ERROR: No data type recognised for {column_name}"

    print(ret_val)
    return ret_val + ','


def parse_data(conn: kuzu.Connection, _data_dir, _node_infile, _edge_infile, _outfile) -> None:
    """
    parses/loads the node/edge JSON data into a Kuzu DB

    :param conn:
    :param _data_dir:
    :param _node_infile:
    :param _edge_infile:
    :param _outfile:
    :return:
    """
    node_count = 0
    edge_count = 0

    with Timer(name="nodes", text="DB nodes loaded in {:.2f}s"):
        logger.debug("Loading nodes into the database...")

        for i in range(1, 4):
            inf = os.path.join(_data_dir, _node_infile + str(i) + '.csv')
            inf = str(inf).replace('\\', '/')

            conn.execute(f'COPY Node FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=true);')

            logger.debug(f"Loaded node file %s into the DB.", inf)

    with Timer(name="edges", text="DB edges loaded in {:.2f}s"):
        logger.debug("Loading edges into the database...")

        for i in range(1,2):
            inf = os.path.join(_data_dir, _edge_infile + str(i) + '.csv')
            inf = str(inf).replace('\\', '/')

            conn.execute(f'COPY Edge FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=true);')

            logger.debug(f"Loaded edge file %s into the DB.", inf)

    logger.debug(f"Successfully loaded nodes and edges into the DB.")


def check_file(infile):
    counter = 0
    with open(infile, 'r', encoding='utf-8') as fh:
        csv_reader = csv.reader(fh)

        for line in csv_reader:
            counter += 1

            if len(line) != 1965:
                logger.debug('line: %s', counter)


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

    # wipe the DB if we are creating tables
    if run_type == "TABLES" and os.path.isdir(db_dir):
        # Delete directory each time until we have MERGE FROM available in kuzu
        shutil.rmtree(db_dir, ignore_errors=True)

    # Create the database
    db = kuzu.Database(db_dir)

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
            parse_data(connection, args.data_dir, args.node_infile, args.edge_infile, args.outfile)

        if run_type == "CHECK":
            check_file(os.path.join(args.data_dir, args.node_infile))
            check_file(os.path.join(args.data_dir, args.edge_infile))

    except Exception as e:
        logger.exception(f'Exception parsing')
    finally:
        connection.close()
