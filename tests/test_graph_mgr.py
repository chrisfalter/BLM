import os
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


@pytest.fixture
def get_temp_file():
    fname = "temp.ncol"
    yield fname
    os.remove(fname)

    
def test_graph_isDirectedWeighted(get_graph_mgr):
    urg = get_graph_mgr
    assert urg.edge_weights[("A","B")] == 2
    verify_all_graph_properties(urg.g)


def verify_all_graph_properties(g):
    assert g.is_weighted()
    assert g.is_directed()
    assert g.is_named()
    assert g.strength(["A", "B", "C", "F"], "IN", weights="weight") == [0,2,0,1]
    assert g.strength(["A", "B", "C", "F"], "OUT", weights="weight") == [2,0,1,0]

    
def test_influenceScores_areSortedCorrectly(get_graph_mgr):
    urg: UserRetweetGraph = get_graph_mgr
    influence_scores = urg.get_influence_scores()
    assert influence_scores["B"] > influence_scores["A"]


def test_load_rehydratesAllVerticesAndWeights(get_graph_mgr, get_temp_file):
    urg: UserRetweetGraph = get_graph_mgr
    tf = get_temp_file
    urg.save_graph(tf)
    urg2 = UserRetweetGraph()
    urg2.load_graph(tf)
    verify_all_graph_properties(urg2.g)
