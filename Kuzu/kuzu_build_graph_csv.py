import os
import shutil
import argparse
import kuzu
import re
from codetiming import Timer
from common.logger import LoggingUtil
import pandas as pd
import csv
import pickle
from collections import defaultdict

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

# list of node category labels (aka classes) in priority order
ordered_categories = ["biolink:GeneFamily", "biolink:Gene", "biolink:Protein", "biolink:SmallMolecule", "biolink:MolecularMixture",
                      "biolink:ChemicalMixture", "biolink:PhenotypicFeature", "biolink:Disease", "biolink:SequenceVariant",
                      "biolink:CellularComponent", "biolink:Cell", "biolink:AnatomicalEntity", "biolink:MolecularActivity",
                      "biolink:BiologicalProcess", "biolink:Pathway", "biolink:OrganismTaxon", "biolink:Phenomenon", "biolink:Procedure",
                      "biolink:Device", "biolink:OrganismAttribute", "biolink:ClinicalAttribute", "biolink:Activity",
                      "biolink:InformationContentEntity", "biolink:ChemicalEntity", "biolink:BiologicalEntity"]

# define the file counter ranges used in the process.
# all data file indexes ranges are nodes: 1-21, edges: 1-24
# note node range 11-12, edge range 1-2 is a good set of data to test with
# use 0 for the upper end of the range to bypass operations on that type of data
node_rng = range(1, 21)
edge_rng = range(1, 24)

# init storage for node class and edge predicate lookup data
node_class_lookups: dict = {}
edge_predicate_lookups = defaultdict(set)


def convert_data(_data_dir, _infile, _file_type) -> None:
    """
    goes through each input file and converts columns to lists or int64 data types, reorder
    node class lists and add/deletes/rename certain columns.

    input file names be of the form: rk-nodes-pt<file number>.csv or rk-edges-pt<file number>.csv
    output file names will be of the form: rk-nodes-conv<file number>.csv or rk-edges-conc<file number>.csv

    :param _data_dir:
    :param _infile:
    :param _file_type:
    :return:
    """
    with Timer(name=_file_type, text="{name} DB files converted in {:.2f}s", logger=logger.debug):
        # specify the range of files to work
        if _file_type == 'NODE':
            rng = node_rng
        elif _file_type == 'EDGE':
            rng = edge_rng
        else:
            raise Exception('Unsupported file type.')

        for i in rng:
            # get the input file path
            inf = os.path.join(_data_dir, _infile + str(i) + '.csv')

            # done so this works in both a windows and linux environment
            inf = str(inf).replace('\\', '/')

            # get the output file path
            out_file = os.path.join(_data_dir, _infile.replace('pt', 'conv') + str(i) + '.csv')

            # done so this works in both a windows and linux environment
            out_file = str(out_file).replace('\\', '/')

            # init the columns to rename in the data
            reformat_rename_cols: dict = {}

            # init the target array list conversion columns
            reformat_list_cols: list = []

            # init the target int32 conversion columns
            reformat_int32_cols: list = []

            # init the target columns to delete from the data
            reformat_del_cols: list = []

            logger.debug("Converting file %s into %s", inf, out_file)

            # load the infile into a panda object
            df = pd.read_csv(inf, low_memory=False)

            # convert the list columns to be Kuzu compatible. e.g: "[]", [1,2,3,...], [1.2,2.3,3.4,...], [txt1,txt2, ...]
            if _file_type == 'NODE':
                # define the cols that need renaming (same for RK and CTD data)
                reformat_rename_cols = {'category': 'labels'}

                # define the target array list columns
                reformat_list_cols = ['labels', 'equivalent_identifiers', 'hgvs']

                # define the target int32 columns (None for CTD data)
                reformat_int32_cols = ['lipinski', 'arom_c', 'sp3_c', 'sp2_c', 'sp_c', 'halogen', 'hetero_sp2_c', 'rotb', 'o_n', 'oh_nh', 'rgb',
                                       'fda_labels']

            elif _file_type == 'EDGE':
                # define the cols that need renaming (same for CTD and RK)
                reformat_rename_cols = {'predicate': 'label'}

                # define the target array list columns
                reformat_list_cols: list = ['p_value', 'supporting_affinities', 'slope', 'publications', 'hetio_source', 'tmkp_ids', 'expressed_in',
                                            'pubchem_assay_ids', 'patent_ids', 'aggregator_knowledge_source', 'category', 'provided_by',
                                            'complex_context', 'has_evidence', 'qualifiers', 'phosphorylation_sites', 'drugmechdb_path_id']

                # define the target int32 columns (none for CTD data)
                reformat_int32_cols: list = ['distance_to_feature']

                # RK data testing only - define the edge columns to delete (none for CTD data)

                # starts deletion at the 6th column. match table creation with the rk-edges.tab-hdr-5cols.temp_csv file
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

                # starts deletion at the 16th column. match table creation with the rk-edges.tab-hdr-15cols.temp_csv file
                # reformat_del_cols: list = ['object_aspect_qualifier', 'object_direction_qualifier', 'qualified_predicate', 'Coexpression',
                #                            'Coexpression_transferred', 'Experiments', 'Experiments_transferred', 'Database', 'Database_transferred',
                #                            'Textmining', 'Textmining_transferred', 'Cooccurance', 'Combined_score', 'species_context_qualifier',
                #                            'hetio_source', 'tmkp_confidence_score', 'sentences', 'tmkp_ids', 'detection_method', 'Homology',
                #                            'expressed_in', 'slope', 'pubchem_assay_ids', 'patent_ids', 'aggregator_knowledge_source', 'id',
                #                            'original_subject', 'category', 'provided_by', 'disease_context_qualifier', 'frequency_qualifier',
                #                            'has_evidence', 'negated', 'original_object', 'score', 'FAERS_llr', 'description', 'NCBITaxon', 'Fusion',
                #                            'has_count', 'has_percentage', 'has_quotient', 'has_total', 'qualifiers', 'stage_qualifier',
                #                            'primaryTarget', 'endogenous', 'anatomical_context_qualifier', 'phosphorylation_sites',
                #                            'onset_qualifier', 'object_specialization_qualifier', 'drugmechdb_path_id', 'complex_context',
                #                            'sex_qualifier', 'object_part_qualifier', 'subject_part_qualifier']

                # starts deletion at the 21st column. match table creation with the rk-edges.tab-hdr-20cols.temp_csv file
                # reformat_del_cols: list = ['Experiments', 'Experiments_transferred', 'Database', 'Database_transferred',
                #                            'Textmining', 'Textmining_transferred', 'Cooccurance', 'Combined_score', 'species_context_qualifier',
                #                            'hetio_source', 'tmkp_confidence_score', 'sentences', 'tmkp_ids', 'detection_method', 'Homology',
                #                            'expressed_in', 'slope', 'pubchem_assay_ids', 'patent_ids', 'aggregator_knowledge_source', 'id',
                #                            'original_subject', 'category', 'provided_by', 'disease_context_qualifier', 'frequency_qualifier',
                #                            'has_evidence', 'negated', 'original_object', 'score', 'FAERS_llr', 'description', 'NCBITaxon', 'Fusion',
                #                            'has_count', 'has_percentage', 'has_quotient', 'has_total', 'qualifiers', 'stage_qualifier',
                #                            'primaryTarget', 'endogenous', 'anatomical_context_qualifier', 'phosphorylation_sites',
                #                            'onset_qualifier', 'object_specialization_qualifier', 'drugmechdb_path_id', 'complex_context',
                #                            'sex_qualifier', 'object_part_qualifier', 'subject_part_qualifier']

                # starts deletion at the 41st column. match table creation with the rk-edges.tab-hdr-40cols.temp_csv file
                # reformat_del_cols: list = ['id', 'original_subject', 'category', 'provided_by', 'disease_context_qualifier', 'frequency_qualifier',
                #                            'has_evidence', 'negated', 'original_object', 'score', 'FAERS_llr', 'description', 'NCBITaxon', 'Fusion',
                #                            'has_count', 'has_percentage', 'has_quotient', 'has_total', 'qualifiers', 'stage_qualifier',
                #                            'primaryTarget', 'endogenous', 'anatomical_context_qualifier', 'phosphorylation_sites',
                #                            'onset_qualifier', 'object_specialization_qualifier', 'drugmechdb_path_id', 'complex_context',
                #                            'sex_qualifier', 'object_part_qualifier', 'subject_part_qualifier']

                # duplicate the subject and object columns into the from and to columns.
                # it is a Kuzu requirement that the first 2 columns be from and to.
                df.insert(loc=0, column='to', value=df['object'])
                df.insert(loc=0, column='from', value=df['subject'])

            # rename any specified columns in the data
            df.rename(columns=reformat_rename_cols, inplace=True)

            # get the node label classes in the right order
            if 'labels' in df:
                df['labels'] = df['labels'].apply(lambda x: '' if pd.isna(x) else reorder_node_classes(x))

            # apply the new formatting for lists
            for col in reformat_list_cols:
                # only do this if the column is there
                if col in df:
                    # split the character seperated string into an array
                    df[col] = df[col].apply(lambda x: [] if pd.isna(x) else '[' + ','.join(map(str, str(x).replace('\'', '`').split(';'))) + ']')

            # apply the new formatting for INT32 data
            for col in reformat_int32_cols:
                # only do this if the column is there
                if col in df:
                    # change the data type
                    df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(int(x)))

            # remove specified columns
            df.drop(columns=reformat_del_cols, inplace=True, axis=1)

            # create the new file
            df.to_csv(out_file, index=False)

            logger.debug(f"%s file %s converted and exported to %s.", _file_type, inf, out_file)


def reorder_node_classes(node_classes: str) -> str:
    """
    reorders the node labels array to put the highest priority in the front.

    :param node_classes:
    :return:
    """
    # convert the string to a list
    class_list: list = list(set(node_classes.split(';')))

    # for each item in the ordered class list
    for oc in ordered_categories:
        # if the class is in the node classes
        if oc in class_list:
            # move the item to the front if it is not there already
            if class_list.index(oc) != 0:
                # move the item to the fron of the list
                class_list.insert(0, class_list.pop(class_list.index(oc)))

            # stop looking when a primary class is found
            break

    # return the result in the original format
    return ';'.join(class_list)


def get_data_lookups(_data_dir, _infile, node_class_list, _file_type):
    """
    this method bins data found in the "converted" files into node class/edge predicate files.

    this method returns the node and edge columns for Kuzu DB tables.

    :param _data_dir:
    :param _infile:
    :param node_class_list:
    :param _file_type:
    :return:
    """

    # specify the range of converted files to process
    if _file_type == 'NODE':
        rng = node_rng
    elif _file_type == 'EDGE':
        rng = edge_rng
    else:
        raise Exception('Unsupported file type.')

    logger.debug(f"Getting {_file_type} data lookups...")

    # save the node and its preferred class for the edge table relationships
    with Timer(name=_file_type, text="The {name} lookup dict created in {:.2f}s", logger=logger.debug):
        if _file_type == 'NODE':
            # init the return value
            ret_val: dict = {}

            # for each file to process
            for i in rng:
                # get the input file path
                inf = os.path.join(_data_dir, _infile + str(i) + '.csv')

                # done so this works in both a windows and linux environment
                inf = str(inf).replace('\\', '/')

                # open the input csv file
                with open(inf, 'r') as file:
                    # read the csv file
                    reader = csv.reader(file)

                    # Skip the header
                    next(reader)

                    # go through each line in the file
                    for row in reader:
                        # get the node class
                        node_id: str = row[0]

                        # get the node class
                        node_class: str = row[2].split(',')[0][1:]

                    # save this pair to the return list
                    ret_val.update({node_id: node_class.split(':')[1]})

        # save the source class/predicate/object class
        elif _file_type == 'EDGE':
            # init the return value
            ret_val: defaultdict = defaultdict(set)

            # for each file to process
            for i in rng:
                # get the input file path
                inf = os.path.join(_data_dir, _infile + str(i) + '.csv')

                # done so this works in both a windows and linux environment
                inf = str(inf).replace('\\', '/')

                # open the input csv file
                with open(inf, 'r') as file:
                    # read the csv file
                    reader = csv.reader(file)

                    # Skip the header
                    next(reader)

                    # go through each line in the file
                    for row in reader:
                        # get the class for the subject and object
                        subject_class = node_class_list.get(row[0], None)
                        object_class = node_class_list.get(row[1], None)

                        # if we found both vertices complete/save the tuple
                        if subject_class and object_class:
                            # save this set. note the predicate is in the 4th column in the CSV file
                            ret_val[row[3].split(':')[1]].add((subject_class, object_class))

    # inform the user something may be amiss
    if len(ret_val) == 0:
        logger.debug('Warning: No lookup data found for %s.', _file_type)

    # return the dict of node id/classes or n-e-n relationships
    return ret_val


def bin_data(_data_dir, _infile, _file_type, node_class_lookup) -> None:
    """
    turns the converted files into files whose data is binned by node class and edge predicates.

    input file names we be of the form: rk-nodes-conv<file number>.csv or rk-edges-conv<file number>.csv
    output file names will be of the form: rk-nodes-bin-<node class>.csv or rk-edges-bin-<edge predicate>.csv

    :param _data_dir:
    :param _infile:
    :param _file_type:
    :param node_class_lookup:
    :return:
    """
    logger.debug('Binning %s data files.', _file_type)

    # init the list of file handles
    open_files: dict = {}

    # set the range for the number of files to process
    if _file_type == 'NODE':
        rng = node_rng
    elif _file_type == 'EDGE':
        rng = edge_rng
    else:
        raise Exception('Unsupported file type.')

    try:
        # loop through the converted files
        for i in rng:
            # get the input file path
            inf = os.path.join(_data_dir, _infile + str(i) + '.csv')

            # done so this works in both a windows and linux environment
            inf = str(inf).replace('\\', '/')

            logger.debug('Binning %s file', inf)

            # open the input csv file
            with open(inf, 'r') as file:
                # read the csv file
                reader = csv.reader(file)

                # save the header
                csv_hdr = next(reader)

                # init the from/to target storage
                from_to: str = ''

                # go through each line in the file
                for row in reader:
                    # get the class or predicate based on the type of file being processed
                    if _file_type == 'NODE':
                        # get the node class
                        class_or_pred = row[2].split(',')[0][1:]
                        class_or_pred = class_or_pred.split(':')[1]
                    else:
                        # get the from/to node classes
                        subject_class = node_class_lookup.get(row[0], None)
                        object_class = node_class_lookup.get(row[1], None)

                        # make sure we get the target node classes
                        if subject_class and object_class:
                            # get the predicate with node classes for the file name
                            class_or_pred = row[3].split(':')[1] + '_' + subject_class + '_' + object_class
                        else:
                            logger.warning('Warning: Could not get subject or object classes for %s. Continuing...', row[3].split(':')[1])
                            continue

                    # get the output file path
                    out_file = os.path.join(_data_dir, _infile.replace('conv', 'bin-') + class_or_pred + '.csv')

                    # done so this works in both a windows and linux environment
                    out_file = str(out_file).replace('\\', '/')

                    # check to see if this file has already been created
                    if open_files.get(out_file, None) is None:
                        # create the file
                        file_handle = open(out_file, mode='w', newline='', encoding='utf-8')

                        # create the file
                        csv_writer = csv.writer(file_handle)

                        # write out the csv file header
                        csv_writer.writerow(csv_hdr)

                        # copy the line to the new destination
                        csv_writer.writerow(row)

                        # put the file handle in the list
                        open_files.update({out_file: [file_handle, csv_writer]})
                    else:
                        # use the existing file's handle
                        csv_writer = open_files[out_file][1]

                        # copy the line to the new destination
                        csv_writer.writerow(row)

    except Exception as e:
        logger.exception(f"Error binning {_file_type} files.", e)
    finally:
        # close all the files that were opened
        [v[0].close() for k, v in open_files.items()]

        # clear the list
        open_files = {}

        logger.debug('Binning %s data files complete.', _file_type)


def create_kuzu_tables(conn: kuzu.Connection, _data_dir, _node_file, _edge_file) -> None:
    """
    creates the node and edge tables in kuzu

    :param _edge_file:
    :param _node_file:
    :param _data_dir:
    :param conn:
    :return:
    """

    node_header_file_name = 'rk-nodes.tab-hdr.temp_csv'
    edge_header_file_name = 'rk-edges.tab-hdr.temp_csv'

    try:
        # get the list of node columns
        n_cols: str = process_csv_header(_data_dir, node_header_file_name, 'NODE')

        # get the set of the node classes
        node_classes: list = sorted(set(node_class_lookups.values()))

        # create a table for each node label class
        for node_class in node_classes:
            # create the tables
            conn.execute(f'CREATE NODE TABLE {node_class}({n_cols}, PRIMARY KEY (id))')
            # logger.debug(f'CREATE NODE TABLE {node_class}({n_cols}, PRIMARY KEY (id))')

        # get the list of edge columns. the table header file must match the number of columns in the data
        e_cols = process_csv_header(_data_dir, edge_header_file_name, 'EDGE')

        # get the set of predicates
        predicate_types = sorted(list(edge_predicate_lookups.keys()))

        # for each predicate type
        for predicate_type in predicate_types:
            # get all the relationships for this predicate
            node_classes_by_predicate = edge_predicate_lookups[predicate_type]

            # sort the node class predicates
            node_classes_by_predicate = sorted(node_classes_by_predicate, key=lambda x: x[0])

            # get the from/to clause
            from_to_clause = ','.join([f'FROM {x[0]} TO {x[1]}' for x in node_classes_by_predicate])

            # table name may have multiple to/from node tables
            conn.execute(f"CREATE REL TABLE {predicate_type}({from_to_clause}, {e_cols})")
            # logger.debug(f"CREATE REL TABLE {predicate_type}({from_to_clause}, {e_cols})")

    except Exception as e:
        logger.exception("Error creating node or edge tables.", e)


def process_csv_header(_data_dir, _infile, _file_type) -> str:
    """
    parses the robokop csv header line and creates

    note that this expects the header from the nodes.temp_csv and edges.temp.csv file.
    this file is tab-delimited and contains data types.
    these data types will be used to get the column data in the correct kuzu format.

    :param _data_dir:
    :param _infile:
    :param _file_type:
    :return:
    """
    with open(os.path.join(_data_dir, _infile), 'r', encoding='utf-8') as in_file:
        # load a file iterator for the edges and nodes
        file_iter: iter = iter(in_file)

        array_split_char = ';'

        ret_val: str = ''

        # get the set of columns that need to be renamed for this file type
        if _file_type == 'NODE':
            rename_cols = {'category': 'labels'}
        elif _file_type == 'EDGE':
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
                    ret_val += get_kuzu_data_conversion(col, array_split_char)

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


def get_kuzu_data_conversion(column_name: str, array_split_char: str) -> str:
    """
    Note that the input data is from the original ORION column header.
    this data is in the format <data name>: <data type> and is used to convert the data in the memgraph import query.

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


def import_data(conn: kuzu.Connection, _data_dir, _node_infile, _edge_infile) -> None:
    """
    parses/loads the node/edge data into a Kuzu DB.
    data is coming in as dat files binned by the node classification (preferred label) while the edge predicate relationships
    will be defined by node class vertexes.

    input files will be of the form: rk-nodes-bin-<node class>.csv or rk-edges-bin-<edge predicate>.csv

    :param conn:
    :param _data_dir:
    :param _node_infile:
    :param _edge_infile:
    :return:
    """
    try:
        with Timer(name="nodes", text="DB nodes loaded in {:.2f}s", logger=logger.debug):
            logger.debug("Loading nodes into the database...")

            # get the sorted set of the node classes
            node_classes: list = sorted(set(node_class_lookups.values()))

            for node_class in node_classes:
                # create the name of the file
                inf = os.path.join(_data_dir, _node_infile + node_class + '.csv')

                # fix path for windows
                inf = str(inf).replace('\\', '/')

                # check to see if the file exists
                if os.path.exists(inf):
                    logger.debug("Loading node file %s into the database...", inf)

                    # import the data file
                    conn.execute(f'COPY {node_class} FROM "{inf}" (HEADER=true, DELIMITER=",", IGNORE_ERRORS=false);')
                else:
                    logger.debug("Node file %s does not exist, skipping...", inf)

        with Timer(name="edges", text="DB edges loaded in {:.2f}s", logger=logger.debug):
            logger.debug("Loading edges into the database...")

            # get the set of predicates
            predicate_types = sorted(list(edge_predicate_lookups.keys()))

            # for each predicate type
            for predicate_type in predicate_types:
                # get all the relationships for this predicate
                node_classes_by_predicate = edge_predicate_lookups[predicate_type]

                # sort the node class predicates
                node_classes_by_predicate = sorted(node_classes_by_predicate, key=lambda x: x[0])

                # for each node subject/object class pair
                for node_class_set in node_classes_by_predicate:
                    # get the node classes
                    subject_class = node_class_set[0]
                    object_class = node_class_set[1]

                    # create the name of the file
                    inf = os.path.join(_data_dir, _edge_infile + predicate_type + '_' + subject_class + '_' + object_class + '.csv')

                    # fix path for windows
                    inf = str(inf).replace('\\', '/')

                    # check to see if the file exists
                    if os.path.exists(inf):
                        logger.debug("Loading edge file %s into the database...", inf)

                        try:
                            # import the data file
                            conn.execute(f"COPY {predicate_type} FROM '{inf}' (from='{subject_class}', to='{object_class}',HEADER=true, DELIMITER=',', IGNORE_ERRORS=true);")
                        except Exception as e:
                            logger.exception(
                                f"Edge import exception detected: Failed to load {predicate_type} {subject_class} to {object_class} edge file {inf}:")

                    else:
                        logger.debug("Edge file %s does not exist, skipping...", inf)

    except Exception as e:
        logger.exception('Exception detected:', e)

    logger.debug(f"Successfully loaded nodes and edges into the DB.")


if __name__ == "__main__":
    """
    command line:
    
    python3 kuzu_build_graph_csv.py --node-infile=rk-orig-node-cols.temp_csv --edge-infile=rk-orig-edge-cols.temp_csv 
    --data-dir=D:/dvols/graph-eval/robokop_data/kuzu --outfile=rk-kuzu-db --type=tables
    
    fastapi pod command line:
    cd /logs
           
    python3 --node-infile=rk-nodes-pt20.csv --edge-infile=rk-edges-pt23.csv --data-dir=D:/dvols/graph-eval/robokop_data/kuzu 
    --outfile=rk-kuzu-db --type=data
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

    # init the DB connection
    connection = None

    # # wipe the DB if we are creating Kuzu tables
    if run_type == "TABLES" and os.path.isdir(db_dir):
        # Delete directory each time until we have MERGE FROM available in kuzu
        shutil.rmtree(db_dir, ignore_errors=True)

    try:
        if run_type == "CONVERT":
            with Timer(name="convert", text="Node data converted in {:.2f}s", logger=logger.debug):
                # perform node file operations
                convert_data(args.data_dir, args.node_infile, 'NODE')

            with Timer(name="convert", text="Edge data converted in {:.2f}s", logger=logger.debug):
                # perform edge file operations
                convert_data(args.data_dir, args.edge_infile, 'EDGE')

        if run_type == "CREATE_LUS":
            #  get the set of node ids and their class tuples
            node_class_lookups = get_data_lookups(args.data_dir, args.node_infile, None, 'NODE')

            # serialize the lookup data into pickle files
            with open(os.path.join(args.data_dir, "serialized_node_classes.pkl"), "wb") as node_pkl_file:
                # noinspection PyTypeChecker
                pickle.dump(node_class_lookups, node_pkl_file)

            # get the set of subject class - edge predicate - object class tuples
            edge_predicate_lookups = get_data_lookups(args.data_dir, args.edge_infile, node_class_lookups, 'EDGE')

            with open(os.path.join(args.data_dir, "serialized_edge_predicates.pkl"), "wb") as edge_pkl_file:
                # noinspection PyTypeChecker
                pickle.dump(edge_predicate_lookups, edge_pkl_file)

        # create the tables if requested
        if run_type == "BIN":
            with Timer(name="bin", text="Node and edge data binned in {:.2f}s", logger=logger.debug):
                # perform node file operations
                bin_data(args.data_dir, args.node_infile, 'NODE', None)

                # deserialize the node lookup data from the pickle file
                with open(os.path.join(args.data_dir, "serialized_node_classes.pkl"), "rb") as node_pkl_file:
                    node_class_lookups = pickle.load(node_pkl_file)

                # perform edge file operations
                bin_data(args.data_dir, args.edge_infile, 'EDGE', node_class_lookups)

        # create the tables if requested
        if run_type == "TABLES":
            # deserialize the node lookup data from the pickle file
            with open(os.path.join(args.data_dir, "serialized_node_classes.pkl"), "rb") as node_pkl_file:
                node_class_lookups = pickle.load(node_pkl_file)

            # deserialize the edge lookup data from the pickle file
            with open(os.path.join(args.data_dir, "serialized_edge_predicates.pkl"), "rb") as edge_pkl_file:
                edge_predicate_lookups = pickle.load(edge_pkl_file)

            with Timer(name="Tables", text="Table definitions created in {:.2f}s", logger=logger.debug):
                # Create the database
                db = kuzu.Database(db_dir, max_db_size=274877906944)

                # get a DB connection
                connection = kuzu.Connection(db)

                # create new node and edge tables
                create_kuzu_tables(connection, args.data_dir, args.node_infile, args.edge_infile)

                # close the DB connection
                connection.close()
                connection = None

        # parse the data if requested
        if run_type == "IMPORT":
            # serialize the node lookup data from the pickle file
            with open(os.path.join(args.data_dir, "serialized_node_classes.pkl"), "rb") as node_pkl_file:
                node_class_lookups = pickle.load(node_pkl_file)

            # deserialize the edge lookup data from the pickle file
            with open(os.path.join(args.data_dir, "serialized_edge_predicates.pkl"), "rb") as edge_pkl_file:
                edge_predicate_lookups = pickle.load(edge_pkl_file)

            # Create the database
            db = kuzu.Database(db_dir, max_db_size=274877906944)

            # get a DB connection
            connection = kuzu.Connection(db)

            # parse the data
            import_data(connection, args.data_dir, args.node_infile, args.edge_infile)

            # close the DB connection
            connection.close()
            connection = None

    except Exception as e:
        logger.exception(f'Exception parsing')
    finally:
        # close the DB connection if it is open
        if connection:
            connection.close()

    logger.debug('Processing complete.')
