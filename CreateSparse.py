import csv
import gzip
import os
import numpy
from scipy.sparse import coo_matrix


def create_sparse_graph_parts():
    print("Start to create sparse graph parts")
    files_list = [f for f in os.listdir(graph_path) if f.startswith("part")]

    for file in files_list:

        row = list()
        col = list()
        csvinput = gzip.open(os.path.join(graph_path, file))
        csv_reader = csv.reader(csvinput, delimiter='\t')
        for line in csv_reader:
            user = int(line[0])
            friends_with_masks = line[1].replace("{(", "").replace(")}", "").split("),(")
            friends = [int(friends_with_mask.split(",")[0]) for friends_with_mask in friends_with_masks]
            for friend in friends:
                row.append(user)
                col.append(friend)

        csvinput.close()

        data = numpy.ones(len(row))
        sparse_matrix = coo_matrix((data, (numpy.array(row), numpy.array(col))),
                                   shape=(USERS_COUNT_REVERSED_GRAPH + 1, USERS_COUNT_REVERSED_GRAPH + 1))
        sparse_matrix = sparse_matrix.tocsr()

        numpy.savez(os.path.join(graph_path, file + "_sparse"), data=sparse_matrix.data, indices=sparse_matrix.indices,
                    indptr=sparse_matrix.indptr,
                    shape=sparse_matrix.shape)

    print("Finish to create sparse graph parts")


def create_sparse_reverse_graph():
    print("Start to read in memory graph")
    files_list = [f for f in os.listdir(graph_path) if f.startswith("part") and f.find("sparse") < 0]

    row = list()
    col = list()

    for file in files_list:
        print file
        csvinput = open(os.path.join(graph_path, file))
        csv_reader = csv.reader(csvinput, delimiter='\t')
        for line in csv_reader:
            user = int(line[0])
            friends_with_masks = line[1].replace("{(", "").replace(")}", "").split("),(")
            friends = [int(friends_with_mask.split(",")[0]) for friends_with_mask in friends_with_masks]
            for friend in friends:
                row.append(friend)
                col.append(user)

        csvinput.close()

    data = numpy.ones(len(row))
    sparse_matrix = coo_matrix((data, (numpy.array(row), numpy.array(col))),
                               shape=(USERS_COUNT_REVERSED_GRAPH + 1, USERS_COUNT_REVERSED_GRAPH + 1))
    sparse_matrix = sparse_matrix.tocsr()

    numpy.savez("reversed_graph_sparse", data=sparse_matrix.data, indices=sparse_matrix.indices,
                indptr=sparse_matrix.indptr, shape=sparse_matrix.shape)

    print("Finish to read in memory graph")


if __name__ == "__main__":
    data_home = "/Users/n.pritykovskaya/AIST/Task3"
    graph_path = os.path.join(data_home, "trainGraph")

    USERS_COUNT_REVERSED_GRAPH = 9418031

    create_sparse_graph_parts()
    create_sparse_reverse_graph()

