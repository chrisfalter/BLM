from sys import stderr

import pytest

from src.graph_mgr import UserRetweetGraph


@pytest.fixture
def get_graph_mgr():
    urg = UserRetweetGraph()
    urg.add_retweet("A", "B")
    urg.add_retweet("A", "B")
    urg.add_retweet("C", "F")
    urg.make_graph()
    return urg    


def test_graph_isDirectedWeighted(get_graph_mgr):
    urg = get_graph_mgr
    assert urg.g.is_weighted()
    assert urg.g.is_directed()
    assert urg.g.is_named()
    assert urg.edge_weights[("A","B")] == 2
    assert urg.g.strength(["A", "B", "C", "F"], "IN", weights="weight") == [0,2,0,1]
    assert urg.g.strength(["A", "B", "C", "F"], "OUT", weights="weight") == [2,0,1,0]


def test_influenceScores_areSortedCorrectly(get_graph_mgr):
    urg: UserRetweetGraph = get_graph_mgr
    influence_scores = urg.get_influence_scores()
    assert influence_scores["B"] > influence_scores["A"]
