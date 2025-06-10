import os
import shutil
import argparse
import kuzu
import re
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

    n_cols: str = process_csv_header(_data_dir, _node_file, 'NODE')

    conn.execute(f"CREATE NODE TABLE Node({n_cols}, PRIMARY KEY (id))")

    e_cols = process_csv_header(_data_dir, _edge_file, 'EDGE')

    conn.execute(f"CREATE REL TABLE Edge(FROM Node TO Node, {e_cols})")


def process_csv_header(_data_dir, _infile, file_type):
    """
    parses the robokop csv header line and creates

    note that this expects the header from the nodes.temp_csv and edges.temp.csv file.


    :param _data_dir:
    :param _infile:
    :param file_type:
    :return:
    """
    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # load a file iterator for the edges and nodes
        file_iter: iter = iter(in_file)

        array_split_char = ';'

        ret_val: str = ''

        # get the set of columns to rename for this file type
        if file_type == 'NODE':
            rename_cols = {'category': 'labels'}
        elif file_type == 'EDGE':
            rename_cols = {'predicate': 'label'}

        try:
            # for each line in the file
            while True:
                # get a line of data
                cols = next(file_iter)

                # split the column header by the tab delimiter
                cols = str(cols).split('\t')

                logger.debug('Number of columns in %s to process: %s', os.path.join(_data_dir, _infile), len(cols))

                for col in cols:
                    ret_val += get_conversion(col, array_split_char)

                # stop processing after the first (header) line
                raise StopIteration

        except StopIteration:
            # rename columns
            for k, v in rename_cols.items():
                ret_val = ret_val.replace(k, v)

            # string off the final comma
            ret_val = ret_val[:-1]

        # return to the caller
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
            ret_val += f"{target_col_name} INT64"  # print(target_col_name, 'INT64')
        case 'float[]':
            ret_val += f"{target_col_name} FLOAT[]"  # print(target_col_name, 'FLOAT[]')
        case 'string[]' | 'LABEL':
            ret_val += f"{target_col_name} STRING[]"  # print(target_col_name, 'STRING[]')
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

    try:
        # specify the range of node/edge files to work
        node_rng = range(1, 21)
        edge_rng = range(1, 24)

        with Timer(name="nodes", text="DB nodes loaded in {:.2f}s"):
            logger.debug("Loading nodes into the database...")

            for i in node_rng:
                inf = os.path.join(_data_dir, _node_infile + str(i) + '.csv')
                inf = str(inf).replace('\\', '/')

                logger.debug("Loading node file %s into the database...", inf)

                conn.execute(f'COPY Node FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=false);')

                logger.debug(f"Loaded node file %s into the DB.", inf)

        with Timer(name="edges", text="DB edges loaded in {:.2f}s"):
            logger.debug("Loading edges into the database...")

            for i in edge_rng:
                inf = os.path.join(_data_dir, _edge_infile + str(i) + '.csv')
                inf = str(inf).replace('\\', '/')

                logger.debug("Loading edge file %s into the database...", inf)

                conn.execute(f'COPY Edge FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=false);')

                logger.debug(f'Loaded edge file %s into the DB.', inf)

    except Exception as e:
        print(e)

    logger.debug(f"Successfully loaded nodes and edges into the DB.")


def convert_file(_data_dir, _infile, file_type):
    with Timer(name="files", text="DB files converted in {:.2f}s"):
        logger.debug(f"Converting {file_type} files...")

        # specify the range of files to work
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

            # init the columns to rename
            reformat_rename_cols: dict = {}

            # init the target array list columns
            reformat_list_cols: list = []

            # init the target int32 columns
            reformat_int32_cols: list = []

            # init the target columns to delete
            reformat_del_cols: list = []

            logger.debug("Converting file %s into %s", inf, out_file)

            # load the infile into a pandas object
            df = pd.read_csv(inf, low_memory=False)

            # convert the list columns to be Kuzu compatible. e.g: "[]", [1,2,3,...], [1.2,2.3,3.4,...], [txt1,txt2, ...]
            if file_type == 'NODE':
                # define the cols that need renaming (same for RK and CTD data)
                reformat_rename_cols = {'category': 'labels'}

                # define the target array list columns
                reformat_list_cols = ['labels', 'equivalent_identifiers', 'hgvs']

                # define the target int32 columns (None for CTD data)
                reformat_int32_cols = ['lipinski', 'arom_c', 'sp3_c', 'sp2_c', 'sp_c', 'halogen', 'hetero_sp2_c', 'rotb', 'o_n', 'oh_nh', 'rgb',
                                       'fda_labels']

            elif file_type == 'EDGE':
                # define the cols that need renaming (same for CTD and RK)
                reformat_rename_cols = {'predicate': 'label'}

                # define the target array list columns
                reformat_list_cols: list = ['p_value', 'supporting_affinities', 'slope', 'publications', 'hetio_source', 'tmkp_ids', 'expressed_in',
                                            'pubchem_assay_ids', 'patent_ids', 'aggregator_knowledge_source', 'category', 'provided_by',
                                            'complex_context', 'has_evidence', 'qualifiers', 'phosphorylation_sites', 'drugmechdb_path_id']

                # define the target int32 columns (none for CTD data)
                reformat_int32_cols: list = ['distance_to_feature']

                # RK data testing only - define the columns to delete (none for CTD data)
                # reformat_del_cols: list = ['agent_type','snpeff_effect','distance_to_feature','publications','p_value','ligand','protein',
                #                            'affinity_parameter','supporting_affinities','affinity','object_aspect_qualifier','object_direction_qualifier',
                #                            'qualified_predicate','Coexpression','Coexpression_transferred','Experiments','Experiments_transferred',
                #                            'Database','Database_transferred','Textmining','Textmining_transferred','Cooccurance','Combined_score',
                #                            'species_context_qualifier','hetio_source','tmkp_confidence_score','sentences','tmkp_ids','detection_method',
                #                            'Homology','expressed_in','slope','pubchem_assay_ids','patent_ids','aggregator_knowledge_source','id',
                #                            'original_subject','category','provided_by','disease_context_qualifier','frequency_qualifier','has_evidence',
                #                            'negated','original_object','score','FAERS_llr','description','NCBITaxon','Fusion','has_count','has_percentage',
                #                            'has_quotient','has_total','qualifiers','stage_qualifier','primaryTarget','endogenous','anatomical_context_qualifier',
                #                            'phosphorylation_sites','onset_qualifier','object_specialization_qualifier','drugmechdb_path_id','complex_context',
                #                            'sex_qualifier','object_part_qualifier','subject_part_qualifier']

                # duplicate the subject and object columns into from and to columns.
                # it is a Kuzu requirement that the first 2 columns be from and to.
                df.insert(loc=0, column='to', value=df['object'])
                df.insert(loc=0, column='from', value=df['subject'])

                # RK data - fill the id column
                # df['id'] = range(start_id, start_id + df.shape[0])

                # RK data - increment the id to be the
                # start_id += df.shape[0]

            # rename any columns in the data
            df.rename(columns=reformat_rename_cols, inplace=True)

            # apply the new formatting for lists
            for col in reformat_list_cols:
                # only do this if the column is there
                if col in df:
                    df[col] = df[col].apply(lambda x: [] if pd.isna(x) else '[' + ','.join(map(str, str(x).replace('\'', '`').split(';'))) + ']')

            # apply the new formatting for INT32 data
            for col in reformat_int32_cols:
                # only do this if the column is there
                if col in df:
                    df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(int(x)))

            # remove specified columns
            df.drop(columns=reformat_del_cols, inplace=True, axis=1)

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

    # wipe the DB if we are creating tables
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

    logger.debug('Processing complete.')
