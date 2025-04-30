import os
import argparse
import json
from codetiming import Timer

"""
Methods to parse ORION data and create an import file to load into a MemGraph DB.

note: the procedure to create/import data is to: 
  - run this program to create the merged json files. see example cli params below
    
  - copy the json file to the memgraph pod
    k -n translator-exp --retries=10 cp <output file name> translator-memgraph-0:/var/lib/memgraph/databases/memgraph/<output file name>

  - execute the following command in the memgraph UI 
    CALL import_util.json("/var/lib/memgraph/databases/memgraph/<output file name>");

  - confirm data loaded properly
    see cypher-cmds.txt for more commands
"""


def merge_nodes_edges(_data_dir, _node_infile, _edge_infile, _outfile, _lines_per_file, _output_file_count):
    """
    Creates a data file that has memgraph nodes and edges.

    note this data must be loaded with the memgraph import_data.json() call

    after processing copy up to the MemGraph server pod
        k -n translator-exp --retries=10 cp nodes.json translator-memgraph-0:/var/lib/memgraph/databases/memgraph/merge.json

    :param _data_dir:
    :param _node_infile:
    :param _edge_infile:
    :param _outfile:
    :param _lines_per_file:
    :param _output_file_count:
    :return:
    """
    # open the data files
    with (open(os.path.join(_data_dir, _edge_infile), 'r', encoding='utf-8') as in_edge_file):

        # init the variables for data capture and output
        out_record = {}
        out_data = []

        # init various counters
        line_counter = 0
        file_counter = 0
        total_node_count = 0
        total_edge_count = 0

        # init various flag conditions
        edges_done = False
        nodes_done = False

        # create a map for the node data
        node_key_map = {'id': 'id', 'category': 'labels'}

        # create a map for the edge data
        edge_key_map = {'subject': 'start', 'object': 'end', 'predicate': 'label'}

        # load a file iterator for the edges
        edge_file_iter = iter(in_edge_file)

        # lines_per_file is the number of lines processed in each input file before an output file is created. using -1 will result in all
        # input lines in each file are output into 1 output file.
        # note that there is approximately a 1/3 node/edge count relationship. this will create residual output files that contain only edges.
        # lines_per_file = 500000

        # output_file_count (for testing only) will limit the number of output files created. using 0 will create all files in lines_per_file sized
        # chunks, a value of > 0 will create at least that many output files.
        # output_file_count = 0

        # generate some text for the user
        file_count_text = f'at least {str(_output_file_count)} output file(s)' if _output_file_count != 0 else "as many files as it takes"
        line_count_text = f'all' if _lines_per_file < 0 else _lines_per_file

        print('\nParsing {lines} input edge(s) with associated nodes per file into {file_count_text}.'.format(lines=line_count_text,
                                                                                                              file_count_text=file_count_text))

        # output the data in chunks
        while True:
            print('\nParsing data for output file {file_count}...'.format(file_count=file_counter + 1))

            # init sets to capture the nodes needed and already collected
            needed_node_set = set()
            captured_node_set = set()

            try:
                # if we are not done processing edges
                if not edges_done:
                    # setup a timer
                    with Timer(name="edges", text="\tEdges parsed in {:.3f}s"):
                        while True:
                            # get a line of data
                            line = next(edge_file_iter)

                            # load the JSON item
                            d_line = json.loads(line)

                            # add the subject and object node names so they can be collected later
                            needed_node_set.add(d_line['subject'])
                            needed_node_set.add(d_line['object'])

                            # remap the data
                            out_record = {edge_key_map.get(k, k): v for k, v in d_line.items() if k in edge_key_map.keys()}

                            # save all property attributes, id and the record type
                            attributes = {"type": "relationship", "id": line_counter, "properties": {k: v for k, v in d_line.items()}}

                            # save the remapped data
                            out_record.update(attributes)

                            # save the data in the output array and default non-ascii characters
                            out_data.append(json.dumps(out_record, ensure_ascii=True))

                            # increment the line counter
                            line_counter += 1

                            # increment the total numer of edges counter
                            total_edge_count += 1

                            # max lines in a chunk reached
                            if line_counter == _lines_per_file:
                                # reset the line counter
                                line_counter = 0

                                # no need to continue
                                break
            except StopIteration:
                # mark edge processing complete
                edges_done = True

                # reset the line counter
                line_counter = 0

            try:
                # if we are not done processing nodes
                if not nodes_done:
                    # have to start at the beginning
                    with open(os.path.join(_data_dir, _node_infile), 'r', encoding='utf-8') as in_node_file:
                        # get a node iterator
                        node_file_iter = iter(in_node_file)

                        # setup a timer
                        with Timer(name="nodes", text="\tNodes parsed in {:.3f}s"):
                            # until we reach the desired number of lines processed
                            while True:
                                # get a line of data
                                line = next(node_file_iter)

                                # load the JSON item
                                d_line = json.loads(line)

                                # if this node needed for an edge
                                if d_line['id'] in needed_node_set:
                                    # if it has not already been captured
                                    if d_line['id'] not in captured_node_set:
                                        # remap the data
                                        out_record = {node_key_map.get(k, k): v for k, v in d_line.items() if k in node_key_map.keys()}

                                        # save all properties and the record type
                                        attributes = {"type": "node", "properties": {k: v for k, v in d_line.items()}}

                                        # save the remapped data
                                        out_record.update(attributes)

                                        # save the data in the output array and default non-ascii characters
                                        # note here that the nodes are placed at the top of the list
                                        out_data.insert(0, json.dumps(out_record, ensure_ascii=True))

                                        # save this node
                                        captured_node_set.add(d_line['id'])

                                        # increment the total number of nodes counter
                                        total_node_count += 1

                                    # increment the line counter
                                    line_counter += 1

                                # max lines in a chunk reached
                                if line_counter == len(needed_node_set):  # line_counter == _lines_per_file or
                                    # reset the line counter
                                    line_counter = 0

                                    # no need to continue
                                    break
            except StopIteration:
                # mark node processing complete
                nodes_done = True

                # reset the line counter
                line_counter = 0

            if out_data:
                # increment the chunk counter
                file_counter += 1

                # convert to text
                out_str = ",".join(out_data)

                # setup a timer
                with Timer(name=f"file_output", text="\tOutput file created in {:.4f}s"):
                    # open up the output file
                    with open(os.path.join(_data_dir, _outfile + f'-{str(_lines_per_file)}-{str(file_counter)}.json'), 'w',
                              encoding='utf-8') as out_file:
                        # start the output
                        out_file.write('[' + out_str + ']')

                        # flush the output file data to disk
                        out_file.flush()

                # reset the data for the next chunk
                out_data = []
                out_str = ""

            # stop if we reached max file count or the end of node and edge data
            if (file_counter == _output_file_count and _output_file_count > 0) or (nodes_done and edges_done):
                break

    print('\nFinal stats: {final_edge_count} edge(s) and {final_node_count} node(s) processed.'.format(final_node_count=total_node_count,
                                                                                                       final_edge_count=total_edge_count))


if __name__ == "__main__":
    """
    command line args for CTD data:
    
--node-infile
D:/dvols/graph-eval/ctd_data/nodes-orig.jsonl
--edge-infile
D:/dvols/graph-eval/ctd_data/edges-orig.jsonl
--data-dir
D:/dvols/graph-eval
--outfile
ctd
--lines_per_file
-1
--output_file_count
0
    """

    """
    command line args for robokop data:
    
--node-infile
D:/dvols/graph-eval/robokop_data/nodes.jsonl
--edge-infile
D:/dvols/graph-eval/robokop_data/edges.jsonl
--data-dir
D:/dvols/graph-eval
--outfile
robokop
--lines_per_file
500000
--output_file_count
1

    lines_per_file is the number of lines processed in each input file before an output file is created. using -1 will result in all
    input lines in each file are output into 1 output file.
    note that edge groups are collected first and then the nodes specified are collected. 
    only edges.

    output_file_count (for testing only) will limit the number of output files created. using 0 will create all files in lines_per_file sized
    chunks, a value of > 0 will create at least that many output files.       
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('--node-infile', dest='node_infile', type=str, help='Node input file')
    parser.add_argument('--edge-infile', dest='edge_infile', type=str, help='Edge input file')
    parser.add_argument('--data-dir', dest='data_dir', type=str, help='Data directory')
    parser.add_argument('--outfile', dest='outfile', type=str, help='Output file')
    parser.add_argument('--lines_per_file', dest='lines_per_file', type=str, help='Number of node/edges processed per file')
    parser.add_argument('--output_file_count', dest='output_file_count', type=str, help='number of output files created')

    args = parser.parse_args()

    # process the node and edge files
    merge_nodes_edges(args.data_dir, args.node_infile, args.edge_infile, args.outfile, int(args.lines_per_file), int(args.output_file_count))
