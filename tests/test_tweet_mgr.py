"""Tests for TweetsManager class. See test_tweets.csv for summary of users/tweets/ralationships in test data"""
from collections import defaultdict, Counter
import json
import pytest
import sys
sys.path.append("src")
from typing import Dict

from src.tweet_mgr import TweetsManager, UserActivity


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
    tw_mgr.analyze_graph()
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


def test_retweetMemeCounter_hasCorrectCounts(get_tw_mgr):
    expected = {}
    expected[("2746297971", "335972576")] = Counter(["shutitdown"])
    expected[("2746297972", "335972576")] = Counter(["shutitdown"])
    expected[("247052159", "2378713202")] = Counter(["ericgarner", "mikebrown"])
    expected[("247052160", "2378713202")] = Counter(["ericgarner", "mikebrown"])
    actual = get_tw_mgr.retweet_meme_counter
    for user_pair in expected:
        assert actual[user_pair] == expected[user_pair]
    assert len(actual) == len(expected)


def test_accountInfluence_isSortedCorrectly(get_communities):
    account_activity_map:Dict[str, UserActivity] = get_communities.user_activity
    for account_activity in account_activity_map.values():
        assert account_activity.influence != 0.0
        assert account_activity.influence_rank != 0
    influencers = ["335972576", "2378713202"]
    followers = ["2746297971", "2746297972", "247052159", "247052160"]
    for influencer in influencers:
        for follower in followers:
            influencer_activity = account_activity_map[influencer]
            follower_activity = account_activity_map[follower]
            assert influencer_activity.influence > follower_activity.influence
            assert influencer_activity.influence_rank < follower_activity.influence_rank
    

def test_communityUserMap_hasCorrectMemberLists(get_communities):
    community_user_map = get_communities.community_user_map
    assert 2 == len(community_user_map)
    for community_id in community_user_map:
        user_list = community_user_map[community_id]
        if "335972576" in user_list:
            assert "2746297971" in user_list
            assert "2746297972" in user_list
            assert 3 == len(user_list)
        elif "2378713202" in user_list:
            assert "247052159" in user_list
            assert "247052160" in user_list
            assert 3 == len(user_list)


def test_userCommunityMap_assignsCorrectCommunities(get_communities):
    community_user_map = get_communities.community_user_map

    # build the expected user->community map from the community->user map
    expected = {}
    for community_id in community_user_map:
        user_list = community_user_map[community_id]
        for user in user_list:
            expected[user] = community_id

    # verify the user->community map
    actual = get_communities.user_community_map
    assert actual == expected


def _get_community_ids(tw_mgr):
    user_comm_map = tw_mgr.user_community_map
    blue_community_id = user_comm_map["2378713202"]
    orange_community_id = user_comm_map["335972576"]
    return blue_community_id, orange_community_id


def test_communityMemeCounter_hasCorrectCounts(get_communities):
    blue_community_id, orange_community_id = _get_community_ids(get_communities)
    expected_orange_memes = Counter(["shutitdown"] * 2)
    expected_blue_memes = Counter(["ericgarner", "mikebrown"] * 2)
    actual_map = get_communities.community_activity_map
    assert actual_map[orange_community_id].meme_counter == expected_orange_memes
    assert actual_map[blue_community_id].meme_counter == expected_blue_memes


def test_communityRetweetCounter_hasCorrectCounts(get_communities):
    blue_community_id, orange_community_id = _get_community_ids(get_communities)
    expected_blue_retweet_counter = Counter([541208741106819072] * 2)
    expected_orange_retweet_counter = Counter([541290893425524736] * 2)
    actual_map = get_communities.community_activity_map
    assert actual_map[blue_community_id].retweet_counter == expected_blue_retweet_counter
    assert actual_map[orange_community_id].retweet_counter == expected_orange_retweet_counter


def test_interCommunityRetweetCounter_hasCorrectCounts(get_communities):
    # there are no inter-community retweets due to the test design
    pass


def test_interCommunityReplyCounter_hasCorrectCounts(get_communities):
    # mock data contains one reply from blue to orange
    blue_community_id, orange_community_id = _get_community_ids(get_communities)
    actual = get_communities.inter_comm_reply_counter
    replies = get_communities.replies
    assert len(replies[("247052159", "335972576")]) == 1
    expected_count = 1
    assert actual[(("247052159", "335972576"), (blue_community_id, orange_community_id))] == expected_count
    expected_count = 0
    assert actual[(("335972576", "247052159"),(orange_community_id, blue_community_id))] == expected_count


def test_communityTweetCounter_hasCorrectCounts(get_communities):
    expected_num = 5 # both communities have 5 tweets in test data
    activity_map = get_communities.community_activity_map
    for community_id in _get_community_ids(get_communities):
        actual_num = activity_map[community_id].num_tweets
        assert actual_num == expected_num
