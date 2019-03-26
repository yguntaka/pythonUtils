from threading import Thread
from Queue import Queue
import time
from time import sleep
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from gremlin_python import statics
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import *

# Path to our graph (this assumes a locally running Gremlin Server)
# Note how the path is a Web Socket (ws) connection.
# Writer Neptune Instance
#endpoint = 'ws://neptunedbcluster-70fbv2otqb11.cluster-c814mxxksbjw.us-east-1.neptune.amazonaws.com:8182/gremlin'
# read replica
endpoint = 'ws://relscisecondneptinst.c814mxxksbjw.us-east-1.neptune.amazonaws.com:8182/gremlin'
# Obtain a graph traversal source using a remote connection
graph=Graph()
g = graph.traversal().withRemote(DriverRemoteConnection(endpoint,'g'))

# No of concurrent threads to run
concurrent = 6

def doWork():           #worker. call grem query from here
    while True:
        v1,v2 = q.get()
        fromV, toV, elapsed = getPaths(v1, v2)    # call grem query, collect results.
        doSomethingWithResult(fromV, toV, elapsed)
        q.task_done()

def getPaths(v1, v2):        # execute gremiln query
    try:
        start = time.time()
        p = g.withSideEffect("Neptune#repeatMode","CHUNKED_DFS").withSack(0).V().hasId(v1). \
            repeat(__.outE().sack(Operator.sum).by('weight').inV().simplePath()).times(3). \
            emit(__.hasId(v2)).hasId(v2).limit(300).order().by(__.sack(),Order.incr). \
            local(__.union(__.path().by(T.id).by('weight'),__.sack()).fold()). \
            toList()
        end = time.time()
        timeDelta = end - start
        return v1,v2,timeDelta
    except Exception as e:
        return "error", str(e)

def doSomethingWithResult(v1, v2, elapsed): # write elapsed  time and later,  paths to file.
    print v1,"\t",v2,":", elapsed, "seconds"


q = Queue(concurrent * 2)       # create queue with list of a-vertices
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:                            # Load from and to vertices into a queue
    for y in open('blist.txt'):
        for x in open('alist.txt'):
            q.put((x.strip(),y.strip()))
    q.join()
except KeyboardInterrupt:
    sys.exit(1)
print "Done"