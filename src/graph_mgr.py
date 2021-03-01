from collections import Counter

import igraph as ig


class UserRetweetGraph():

    def __init__(self):
        self.edge_weights = Counter()

    def add_retweet(self, retweeter_user_id, retweeted_user_id):
        self.edge_weights[(str(retweeter_user_id), str(retweeted_user_id))] += 1

    def make_graph(self, directed = True):
        edges = [(key[0], key[1], self.edge_weights[key]) for key in self.edge_weights]
        self.g = ig.Graph.TupleList(edges, directed=directed, edge_attrs=["weight"])

