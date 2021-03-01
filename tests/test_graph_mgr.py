from sys import stderr

import pytest

from src.graph_mgr import UserRetweetGraph

def test_graph_isDirectedWeighted():
    urg = UserRetweetGraph()
    urg.add_retweet("A", "B")
    urg.add_retweet("A", "B")
    urg.add_retweet("C", "F")
    urg.make_graph()
    assert urg.g.is_weighted()
    assert urg.g.is_directed()
    assert urg.g.is_named()
    assert urg.edge_weights[("A","B")] == 2
    assert urg.g.strength(["A", "B", "C", "F"], "IN", weights="weight") == [0,2,0,1]
    assert urg.g.strength(["A", "B", "C", "F"], "OUT", weights="weight") == [2,0,1,0]

    stderr.write(str(urg.g.es))
    stderr.write('\n')
    stderr.write(str(urg.g.vs))
    urg.g.degree(["A"], mode = "IN")
    urg.g.degree(["A"], mode = "in")
    urg.g.degree(["A"], type='in')