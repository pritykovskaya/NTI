import os
import multiprocessing
import numpy
import scipy
import time
from scipy import sparse
from scipy.sparse import csr_matrix, dok_matrix


def read_sparse_matrix(file_path):
    print("Start reading")
    loader = numpy.load(file_path)
    mtx = csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape=loader['shape'])
    print("Finish reading")
    return mtx


def initProcess(xdata, xindices, xindptr, xshape, wdata, windices, windptr, wshape):

    global XData
    global XIndices
    global XIntptr
    global Xshape

    XData = xdata
    XIndices = xindices
    XIntptr = xindptr
    Xshape = xshape

    global WData
    global WIndices
    global WIntptr
    global Wshape

    WData = wdata
    WIndices = windices
    WIntptr = windptr
    Wshape = wshape


def dot2(args):

    rowInds, i = args

    print "Start " + str(i)

    global XData
    global XIndices
    global XIntptr
    global Xshape

    Xdata = numpy.frombuffer(XData, dtype=numpy.float)
    Xindices = numpy.frombuffer(XIndices, dtype=numpy.int32)
    Xindptr = numpy.frombuffer(XIntptr, dtype=numpy.int32)
    Xr = scipy.sparse.csr_matrix((Xdata, Xindices, Xindptr), shape=Xshape)

    global WData
    global WIndices
    global WIntptr
    global Wshape

    Wdata = numpy.frombuffer(WData, dtype=numpy.float)
    Windices = numpy.frombuffer(WIndices, dtype=numpy.int32)
    Windptr = numpy.frombuffer(WIntptr, dtype=numpy.int32)
    Wr = scipy.sparse.csr_matrix((Wdata, Windices, Windptr), shape=Wshape)

    return Xr[rowInds[i]:rowInds[i+1], :].dot(Wr)


def getMatmat(X):
    numJobs = multiprocessing.cpu_count() - 2
    rowInds = numpy.array(numpy.linspace(0, X.shape[0], numJobs+1), numpy.int)

    #Store the data in X as RawArray objects so we can share it amoung processes
    XData = multiprocessing.RawArray("d", X.data)
    XIndices = multiprocessing.RawArray("i", X.indices)
    XIndptr = multiprocessing.RawArray("i", X.indptr)

    def matmat(W):
        WData = multiprocessing.RawArray("d", W.data)
        WIndices = multiprocessing.RawArray("i", W.indices)
        WIndptr = multiprocessing.RawArray("i", W.indptr)

        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=initProcess, initargs=(XData, XIndices, XIndptr, X.shape, WData, WIndices, WIndptr, W.shape))
        params = []

        for i in range(numJobs):
            params.append((rowInds, i))

        iterator = pool.map(dot2, params)
        sumMat = csr_matrix((rowInds[1] - rowInds[0], USERS_COUNT_REVERSED_GRAPH + 1))

        for i in range(numJobs):
            sumMat = sumMat + iterator[i]

        return sumMat

    return matmat

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

    for i in xrange(16):
        print i
        if i > 9:
            input = 'part-v024-o000-r-000' + str(i) + "_sparse.npz"
        else:
            input = 'part-v024-o000-r-0000' + str(i) + "_sparse.npz"

        graph_part = read_sparse_matrix(os.path.join(graph_path, input))
        mtx = getMatmat(graph_part)(reversed_graph)
        numpy.savez(os.path.join(graph_path, "mult_" + input), data=mtx.data,
                    indices=mtx.indices,
                    indptr=mtx.indptr,
                    shape=mtx.shape)

        print "Finish " + str(i)

    parallelTime = time.time() - startTime
    print "Finish parallel"
    print parallelTime

