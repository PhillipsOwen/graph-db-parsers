import os
import shutil
import argparse
import kuzu
from codetiming import Timer
import json
from common.logger import LoggingUtil

"""
this code takes the node/edge jsonl files and parses them into a Kuzu DB
using JSON import libraries

powen, 2025-04-22 
"""
# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("kuzu_build_graph_json", level=log_level, line_format='medium', log_file_path=log_path)

# the location of this file
test_dir = os.path.dirname(os.path.abspath(__file__))


def create_node_table(conn: kuzu.Connection) -> None:
    """
    creates the node table in kuzu

    :param conn:
    :return:
    """

    conn.execute("""
        CREATE NODE TABLE Node(
                id STRING PRIMARY KEY,
                labels STRING[],
                properties JSON,
                equivalent_identifiers STRING[] 
        )
        """)


def create_edge_table(conn: kuzu.Connection) -> None:
    """
    Creates the edge table in kuzu

    :param conn:
    :return:
    """

    conn.execute("""
        CREATE REL TABLE
            Edge(
                FROM Node TO Node,
                label STRING,      
                id INT64,
                properties JSON,     
                knowledge_level STRING,
                agent_type STRING
            )
        """)


def load_json(conn: kuzu.Connection):
    """
    loads the JSON loader object into Kuzu

    :param conn:
    :return:
    """

    conn.execute("""
        INSTALL json;
        LOAD json;
    """)


def process_node_file(_data_dir, _infile, _outfile):
    """
    parses the node JSON file and creates another JSON file for the Kuzu DB

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file, open(os.path.join(_data_dir, _outfile), 'w',
                                                                                         encoding='utf-8') as out_file):
        d_line = {}
        out_data = []
        line_counter = 0

        node_key_map: dict = {'id': 'id', 'category': 'labels'}

        logger.debug('Parsing nodes...')

        out_file.write('[')

        for line in in_file:
            d_line = json.loads(line)

            # remap the data
            out_record = {node_key_map.get(k, k): v for k, v in d_line.items() if k in node_key_map.keys()}

            # save all properties and the record type
            attributes = {"properties": {k: v for k, v in d_line.items()}}

            # save the remapped data
            out_record.update(attributes)

            # out_data.append(json.dumps(d_line, ensure_ascii=True))

            if line_counter == 0:
                out_file.write(json.dumps(out_record, ensure_ascii=True))
                first_record = False
            else:
                out_file.write(',' + json.dumps(out_record, ensure_ascii=True))

            line_counter += 1

            # out_file.flush()

            # if line_counter > 10000:  #     break

        out_file.write(']')

    return line_counter


def process_edge_file(_data_dir, _infile, _outfile):
    """
    parses the edge JSON file and creates another JSON file for the Kuzu DB

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file, open(os.path.join(_data_dir, _outfile), 'w',
                                                                                         encoding='utf-8') as out_file):
        d_line = {}
        out_data = []
        line_counter = 0

        edge_key_map = {'subject': 'from', 'object': 'to', 'predicate': 'label'}

        logger.debug('Parsing edges...')

        out_file.write('[')

        for line in in_file:
            d_line = json.loads(line)

            out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

            # save all property attributes, id and the record type
            attributes = {"id": line_counter, "properties": {k: v for k, v in d_line.items()}}

            # save the remapped data
            out_record.update(attributes)

            # d_line.pop('subject', None)
            # d_line.pop('object', None)

            if d_line.get('publications') is None:
                d_line['publications'] = []
            elif d_line['publications'] is not None and type(d_line['publications']) is not list:
                d_line['publications'] = [d_line['publications']]

            out_record.update(d_line)

            if line_counter == 0:
                out_file.write(json.dumps(out_record, ensure_ascii=True))
                first_record = False
            else:
                out_file.write(',' + json.dumps(out_record, ensure_ascii=True))

            line_counter += 1

            # out_file.flush()

        out_file.write(']')

    return line_counter


def parse_data(conn: kuzu.Connection, _node_infile, _edge_infile, _data_dir, _outfile, _load_db_only) -> None:
    """
    parses/loads the node/edge JSON data into a Kuzu DB

    :param conn:
    :param _node_infile:
    :param _edge_infile:
    :param _data_dir:
    :param _outfile:
    :param _load_db_only:
    :return:
    """
    node_count = 0
    edge_count = 0

    # init the connection for loading
    load_json(conn)

    with Timer(name="nodes", text="Nodes parsed/loaded in {:.2f}s"):
        # Nodes
        if not _load_db_only:
            node_count = process_node_file(_data_dir, _node_infile, _outfile + '-nodes.json')
        else:
            logger.debug('Skipped processing input node file...')

        nf = os.path.join(_data_dir, _outfile + '-nodes.json')
        # nf = str(nf).replace('\\', '/')

        create_node_table(conn)

        logger.debug("Loading nodes into the database...")

        conn.execute(f"COPY Node FROM '{nf}';")

    with Timer(name="edges", text="Edges parsed/loaded in {:.2f}s"):
        # Edges
        if not _load_db_only:
            edge_count = process_edge_file(_data_dir, _edge_infile, _outfile + '-edges.json')
        else:
            logger.debug('Skipped processing input edge file...')

        ef = os.path.join(_data_dir, _outfile + '-edges.json')
        # ef = str(ef).replace('\\', '/')

        create_edge_table(conn)

        logger.debug("Loading edges into the database...")

        conn.execute(f"COPY Edge FROM '{ef}';")

    logger.debug(f"Successfully loaded %s nodes and %s edges into the DB.", node_count, edge_count)


if __name__ == "__main__":
    """
    command line:
    
    python3 kuzu_build_graph_json.py --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=D:/dvols/graph-eval/ctd_data/
    
    fastapi pod command line:
    cd /logs
           
    python3 kuzu_build_graph_json.py --node-infile=rk-nodes.jsonl --edge-infile=rk-edges.jsonl --data-dir=graph-eval <--load-db-only>
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--load-db-only', dest='load_db_only', type=bool, help='Only load the DB, use existing data')

    args = parser.parse_args()

    db_dir: str = os.path.join(args.data_dir, str(args.outfile))

    # Delete directory each time till we have MERGE FROM available in kuzu
    shutil.rmtree(db_dir, ignore_errors=True)

    # Create the database
    db = kuzu.Database(db_dir)
    connection = kuzu.Connection(db)

    try:
        parse_data(connection, args.node_infile, args.edge_infile, args.data_dir, args.outfile, args.load_db_only)
    except Exception as e:
        logger.exception(f'Exception parsing')
    finally:
        connection.close()
