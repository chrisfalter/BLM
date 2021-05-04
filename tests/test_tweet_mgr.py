"""Tests for TweetsManager class. See test_tweets.csv for summary of users/tweets/ralationships in test data"""
import json
import pytest
import sys
sys.path.append("src")

from tweet_mgr import TweetsManager


@pytest.fixture
def get_tw_mgr():
    with open("tests/test_tweets.json", encoding='utf-8') as f:
        tweets = json.load(f)
    tw_mgr = TweetsManager()
    for tweet in tweets:
        tw_mgr.process_tweet(tweet)
    tw_mgr.process_deferred_interactions()
    return tw_mgr


@pytest.fixture
def get_retweet_graph(get_tw_mgr):
    get_tw_mgr.urg.make_graph()
    return get_tw_mgr.urg


def test_userActivity_hasCorrectCounts(get_tw_mgr):
    userActivityMap = get_tw_mgr.user_activity
    assert 6 == len(userActivityMap) # 6 users have tweeted
    assert userActivityMap["335972576"].tweet_count == 2
    assert userActivityMap["335972576"].retweeted_count == 2
    assert userActivityMap["335972576"].replied_to_count == 1
    assert userActivityMap["247052159"].tweet_count == 3
    assert userActivityMap["247052159"].retweet_count == 1
    assert userActivityMap["247052159"].reply_count == 2


def test_inactiveUser_notInUserActivityMap(get_tw_mgr):
    userActivityMap = get_tw_mgr.user_activity
    inactiveUser = "335972575" # was retweeted and replied to, but didn't tweet
    assert inactiveUser not in userActivityMap


def test_retweetGraph_nodes_haveCorrectDegrees(get_retweet_graph):
    g = get_retweet_graph.g
    retweet_users = [
       "2746297971",
       "2746297972",
       "335972576",
       "247052159",
       "247052160",
       "2378713202",
    ]
    # nodes
    node_names = g.vs["name"]
    for retweet_user in retweet_users:
        assert retweet_user in node_names
    assert len(g.vs) == 6

    # in-degrees
    expected_in_degrees = [0, 0, 2, 0, 0, 2]
    actual_in_degrees = g.strength(retweet_users, "in", weights="weight")
    assert actual_in_degrees == expected_in_degrees

    # out-degrees
    expected_out_degrees = [1, 1, 0, 1, 1, 0]
    actual_out_degrees = g.strength(retweet_users, "out", weights="weight")
    assert actual_out_degrees == expected_out_degrees


def test_retweetGraph_inactiveUser_notInRetweetGraph(get_retweet_graph):
    g = get_retweet_graph.g
    inactiveUser = "335972575" # was retweeted and replied to, but didn't tweet
    assert inactiveUser not in g.vs
