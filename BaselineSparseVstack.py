import os
import multiprocessing
import numpy
import scipy
import time
from scipy import sparse
from scipy.sparse import csr_matrix, dok_matrix, vstack


def read_sparse_matrix(file_path):
    print("Start reading")
    loader = numpy.load(file_path)
    mtx = csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape=loader['shape'])
    print("Finish reading")
    return mtx


def initProcess(xdata, xindices, xindptr, xshape):
    global XData
    global XIndices
    global XIntptr
    global Xshape

    XData = xdata
    XIndices = xindices
    XIntptr = xindptr
    Xshape = xshape


def dot2(i):

    if i > 9:
        part_path = 'part-v024-o000-r-000' + str(i) + "_sparse.npz"
    else:
        part_path = 'part-v024-o000-r-0000' + str(i) + "_sparse.npz"

    print "Start " + part_path

    global XData
    global XIndices
    global XIntptr
    global Xshape

    Xdata = numpy.frombuffer(XData, dtype=numpy.float)
    Xindices = numpy.frombuffer(XIndices, dtype=numpy.int32)
    Xindptr = numpy.frombuffer(XIntptr, dtype=numpy.int32)
    Xr = scipy.sparse.csr_matrix((Xdata, Xindices, Xindptr), shape=Xshape)

    Wr = read_sparse_matrix(os.path.join(graph_path, part_path))

    print "Before mult"
    return Wr.dot(Xr)


def getMatmat(X):
    # Store the data in X as RawArray objects so we can share it amoung processes
    XData = multiprocessing.RawArray("d", X.data)
    XIndices = multiprocessing.RawArray("i", X.indices)
    XIndptr = multiprocessing.RawArray("i", X.indptr)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=initProcess,
                                initargs=(XData, XIndices, XIndptr, X.shape))
    params = range(16)
    print params

    results = pool.map(dot2, params)



if __name__ == "__main__":

    data_home = "/Users/n.pritykovskaya/AIST/Task3/"
    graph_path = os.path.join(data_home, "trainGraph")
    reversed_graph_path = os.path.join(data_home, "reversed_graph_sparse.npz")
    common_friends_count = os.path.join(data_home, "common_friends_count")
    prediction_path = os.path.join(data_home, "prediction")

    USERS_COUNT_GRAPH = 9417921  # 107182
    USERS_COUNT_REVERSED_GRAPH = 9418031  # 8398377

    reversed_graph = read_sparse_matrix(reversed_graph_path)

    startTime = time.time()
    print "Start parallel"
    getMatmat(reversed_graph)
    parallelTime = time.time() - startTime
    print "Finish parallel"
    print parallelTime
