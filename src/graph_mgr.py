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

    def get_influence_scores(self):
        """Return a dict of account_id -> PageRank"""
        scores = self.g.pagerank(weights="weight")
        accounts = [v['name'] for v in self.g.vs]
        results = {}
        for account, score in zip(accounts, scores):
            results[account] = score
        return results
