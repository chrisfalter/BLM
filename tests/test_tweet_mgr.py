"""Tests for TweetsManager class. See test_tweets.csv for summary of users/tweets/ralationships in test data"""
from collections import defaultdict, Counter
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


@pytest.fixture
def get_communities(get_tw_mgr):
    tw_mgr = get_tw_mgr
    tw_mgr.analyze_communities()
    return tw_mgr


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
    node_names = g.vs["name"]
    inactiveUser = "335972575" # was retweeted and replied to, but didn't tweet
    assert inactiveUser not in node_names

def test_userActivity_memeCounter_hasCorrectCounts(get_tw_mgr):
    expected = {}
    expected["335972576"] = Counter(["shutitdown", "shuititdown", "coast2coast"])
    expected["247052159"] = Counter(["shuititdown", "coast2coast"] * 2 + ["ericgarner", "mikebrown"])
    expected["2746297971"] = Counter(["shutitdown"] * 2)
    tm = get_tw_mgr
    for user in expected:
        expected_memes = expected[user]
        actual_memes = tm.user_activity[user].meme_counter
        assert actual_memes == expected_memes


def test_retweetDict_hasCorrectCounts(get_tw_mgr):
    expected_retweets = {}
    expected_retweets[("2746297971", "335972576")] = [541290893425524736]
    expected_retweets[("2746297971", "335972575")] = []
    actual_retweets = get_tw_mgr.retweets
    for user_pair in expected_retweets:
        actual_retweets[user_pair] == expected_retweets[user_pair]


def test_retweetMemeCounter_hasCorrectCounts(get_retweet_graph):
    pass


def test_communityUserMap_hasCorrectMemberLists(get_communities):
    pass


def test_userCommunityMap_assignsCorrectCommunities(get_communities):
    pass


def test_communityMemeCounter_hasCorrectCounts(get_communities):
    pass


def test_communityRetweetCounter_hasCorrectCounts(get_communities):
    pass


def test_interCommunityRetweetCounter_hasCorrectCounts(get_communities):
    pass


def test_interCommunityReplyCounter_hasCorrectCounts(get_communities):
    pass


def test_communityTweetCounter_hasCorrectCounts(get_communities):
    pass
