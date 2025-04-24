import os
import shutil
import argparse
import kuzu
from codetiming import Timer
import json

"""
this code takes the CTD node/edge jsonl files and parses them into a Kuzu DB
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
        CREATE NODE TABLE CTDNode(
                id STRING PRIMARY KEY,
                name STRING,
                category STRING[],                
                equivalent_identifiers STRING[], 
                NCBITaxon STRING, 
                information_content STRING,
                description STRING                
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


def create_edge_table(conn: kuzu.Connection) -> None:
    """
    Creates the edge table in kuzu

    :param conn:
    :return:
    """

    conn.execute("""
        CREATE REL TABLE
            CTDEdge(
                FROM CTDNode TO CTDNode,
                predicate STRING,
                primary_knowledge_source STRING,
                publications STRING[],
                NCBITaxon STRING,
                description STRING,                
                knowledge_level STRING,
                agent_type STRING,
                object_aspect_qualifier STRING,
                object_direction_qualifier STRING,
                qualified_predicate STRING                               
            )
        """)


def process_node_file(_data_dir, _infile, _outfile):
    """
    parses the node JSON file and creates another JSON file for the Kuzu DB

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        d_line = {}
        out_data = []
        line_counter = 0

        for line in in_file:
            d_line = json.loads(line)

            out_data.append(json.dumps(d_line, ensure_ascii=True))

            line_counter += 1

            # if line_counter > 10000:
            #     break

        out_str = ", ".join(out_data)

        out_file.write('[' + out_str + ']')


def process_edge_file(_data_dir, _infile, _outfile):
    """
    parses the edge JSON file and creates another JSON file for the Kuzu DB

    :param _data_dir:
    :param _infile:
    :param _outfile:
    :return:
    """
    with (open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file,
          open(os.path.join(_data_dir, _outfile), 'w', encoding='utf-8') as out_file):

        d_line = {}
        out_data = []
        line_counter = 0

        edge_key_map = {'subject': 'from', 'object': 'to'}

        for line in in_file:
            d_line = json.loads(line)

            out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

            d_line.pop('subject', None)
            d_line.pop('object', None)

            out_record.update(d_line)

            out_data.append(json.dumps(out_record, ensure_ascii=True))

        out_str = ", ".join(out_data)

        out_file.write('[' + out_str + ']')


def main(conn: kuzu.Connection, _data_dir, _node_infile, _edge_infile) -> None:
    """
    Loads the node/edge JSON data into the Kuzu DB

    :param _data_dir:
    :param conn:
    :param _node_infile:
    :param _edge_infile:
    :return:
    """
    load_json(conn)

    with Timer(name="nodes", text="CTD Nodes loaded in {:.4f}s"):
        # Nodes
        create_node_table(conn)

        process_node_file(_data_dir, _node_infile, 'kuzu_node_out.json')

        nf = os.path.join(_data_dir, 'kuzu_node_out.json')
        nf = str(nf).replace('\\', '/')

        conn.execute(f"COPY CTDNode FROM '{nf}';")

    with Timer(name="edges", text="CTD Edges loaded in {:.4f}s"):
        # Edges
        create_edge_table(conn)

        process_edge_file(_data_dir, _edge_infile, 'kuzu_edge_out.json')

        ef = os.path.join(_data_dir, 'kuzu_edge_out.json')
        ef = str(ef).replace('\\', '/')

        conn.execute(f"COPY CTDEdge FROM '{ef}';")

    print("Successfully loaded nodes and edges into the DB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')

    args = parser.parse_args()

    DB_NAME = "kuzu-ctd-db"

    # Delete directory each time till we have MERGE FROM available in kuzu
    shutil.rmtree(DB_NAME, ignore_errors=True)

    # Create the database
    db = kuzu.Database(f"./{DB_NAME}")
    connection = kuzu.Connection(db)

    try:
        main(connection, args.data_dir, args.node_infile, args.edge_infile)
    except Exception as e:
        print(e)
    finally:
        connection.close()
