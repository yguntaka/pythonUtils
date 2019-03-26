# Connect to a Gremlin Server using a remote connection and issue some basic queries.
# Import some classes we will need to talk to our graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from gremlin_python import statics
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import *

# Path to our graph (this assumes a locally running Gremlin Server)
# Note how the path is a Web Socket (ws) connection.
endpoint = 'ws://neptunedbcluster-70fbv2otqb11.cluster-c814mxxksbjw.us-east-1.neptune.amazonaws.com:8182/gremlin'

# Obtain a graph traversal source using a remote connection
graph=Graph()
g = graph.traversal().withRemote(DriverRemoteConnection(endpoint,'g'))
v1='5031468'
v2='3140001'
# rel-sci specific code to loop through two files.
res = g.withSideEffect("Neptune#repeatMode","CHUNKED_DFS").withSack(0).V().hasId(v1). \
        repeat(__.outE().sack(Operator.sum).by('weight').inV().simplePath()).times(3). \
        emit(__.hasId(v2)).hasId(v2).limit(300).order().by(__.sack(),Order.incr). \
        local(__.union(__.path().by(T.id).by('weight'),__.sack()).fold()). \
        toList()

print res