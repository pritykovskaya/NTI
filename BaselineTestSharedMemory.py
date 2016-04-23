import os
import multiprocessing
import numpy
import scipy
import time
from scipy import sparse
from scipy.sparse import csr_matrix


def read_sparse_matrix(file_path):
    print("Start reading")
    loader = numpy.load(file_path)
    mtx = csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape=loader['shape'])
    print("Finish reading")
    return mtx

def initProcessRed(xdata, xindices, xindptr, xshape):

    global XData
    global XIndices
    global XIntptr
    global Xshape

    XData = xdata
    XIndices = xindices
    XIntptr = xindptr
    Xshape = xshape


def read_from_shared(i):

    print "Start " + str(i)

    global XData
    global XIndices
    global XIntptr
    global Xshape

    Xdata = numpy.frombuffer(XData, dtype=numpy.float)
    Xindices = numpy.frombuffer(XIndices, dtype=numpy.int32)
    Xindptr = numpy.frombuffer(XIntptr, dtype=numpy.int32)
    Xr = scipy.sparse.csr_matrix((Xdata, Xindices, Xindptr), shape=Xshape)

    return max(Xr)


def run_parallel(X):
    numJobs = 10
    # Store the data in X as RawArray objects so we can share it amoung processes
    print "Start to put data to shared memory"
    XData = multiprocessing.RawArray("d", X.data)
    XIndices = multiprocessing.RawArray("i", X.indices)
    XIndptr = multiprocessing.RawArray("i", X.indptr)
    print "Finish to put data to shared memory"

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=initProcessRed,
                                initargs=(XData, XIndices, XIndptr, X.shape))

    pool.map(read_from_shared, range(numJobs))



if __name__ == "__main__":

    data_home = "/Users/n.pritykovskaya/AIST/Task3/"
    graph_path = os.path.join(data_home, "trainGraph")
    reversed_graph_path = os.path.join(data_home, "reversed_graph_sparse.npz")

    startTime = time.time()
    print "Start parallel"

    reversed_graph = read_sparse_matrix("reversed_graph_sparse.npz")
    run_parallel(reversed_graph)
    parallelTime = time.time() - startTime
    print "Finish parallel"
    print parallelTime

