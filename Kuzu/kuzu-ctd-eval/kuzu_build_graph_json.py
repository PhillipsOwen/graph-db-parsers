import os
import shutil
import argparse
import kuzu
from codetiming import Timer
import json

"""
this code takes the node/edge jsonl files and parses them into a Kuzu DB
using JSON import libraries

powen, 2025-04-22 
"""

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

        print('Parsing nodes...')

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

        print('Parsing edges...')

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
            elif  d_line['publications'] is not None and type(d_line['publications']) is not list:
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


def parse_data(conn: kuzu.Connection, _data_dir, _node_infile, _edge_infile, _load_db_only) -> None:
    """
    parses/loads the node/edge JSON data into a Kuzu DB

    :param _data_dir:
    :param conn:
    :param _node_infile:
    :param _edge_infile:
    :return:
    """
    node_count = 0
    edge_count = 0

    # init the connection for loading
    load_json(conn)

    with Timer(name="nodes", text="Nodes parsed/loaded in {:.4f}s"):
        # Nodes
        if not _load_db_only:
            node_count = process_node_file(_data_dir, _node_infile, 'kuzu_node_out.json')
        else:
            print('Skipped processing node file...')

        nf = os.path.join(_data_dir, 'kuzu_node_out.json')
        # nf = str(nf).replace('\\', '/')

        create_node_table(conn)

        print("Loading nodes into the database...")

        conn.execute(f"COPY Node FROM '{nf}';")

    with Timer(name="edges", text="Edges parsed/loaded in {:.4f}s"):
        # Edges
        if not _load_db_only:
            edge_count = process_edge_file(_data_dir, _edge_infile, 'kuzu_edge_out.json')
        else:
            print('Skipped processing edge files...')

        ef = os.path.join(_data_dir, 'kuzu_edge_out.json')
        # ef = str(ef).replace('\\', '/')

        create_edge_table(conn)

        print("Loading edges into the database...")

        conn.execute(f"COPY Edge FROM '{ef}';")

    print(f"Successfully loaded {node_count} nodes and {edge_count} edges into the DB.")


if __name__ == "__main__":
    """
    command line:
    
    python3 kuzu_build_graph_json.py --node-infile=nodes-orig.jsonl --edge-infile=edges-orig.jsonl --data-dir=D:/dvols/graph-eval/ctd_data/
    
    fastapi pod command line:
    cd /logs
           
    python3 kuzu_build_graph_json.py --node-infile=rk-nodes.jsonl --edge-infile=rk-edges.jsonl --data-dir=graph-eval --load-db-only=true
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--load-db-only', dest='load_db_only', type=bool, help='Only load the DB, use existing data')

    args = parser.parse_args()

    DB_NAME = "kuzu-db"

    db_dir: str = os.path.join(args.data_dir, DB_NAME)

    # Delete directory each time till we have MERGE FROM available in kuzu
    shutil.rmtree(db_dir, ignore_errors=True)

    # Create the database
    db = kuzu.Database(db_dir)
    connection = kuzu.Connection(db)

    try:
        parse_data(connection, args.data_dir, args.node_infile, args.edge_infile, args.load_db_only)
    except Exception as e:
        print(f'Exception parsing: {e}')
    finally:
        connection.close()
