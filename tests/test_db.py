import pytest
from random import random
import sys
sys.path.append("src")
sys.path.append("tests")
from typing import Dict

from src.blm_activity_db import BlmActivityDb
from src.tweet_mgr import (
    CommunityActivity, 
    CommunityReply,
    CommunityRetweet,
    UserActivity
)
from tests.test_tweet_mgr import get_communities, get_tw_mgr


_period = 0


# These inter-community activity dictionaries do not correspond to the individual
# tweets. They are fabricated simply to test inter-community activity tables.
inter_comm_retweets = {CommunityRetweet(0, 1): 4, CommunityRetweet(1, 0): 3}
inter_comm_replies = {CommunityReply(0, 1): 8, CommunityReply(1, 0): 7}

@pytest.fixture
def get_db(get_communities) -> BlmActivityDb:
    db = BlmActivityDb(":memory:", initialize_db=True)
    tw_mgr = get_communities
    tw_mgr.inter_comm_retweet_counter = inter_comm_retweets
    tw_mgr.inter_comm_reply_counter = inter_comm_replies
    db.save_tweets_mgr(tw_mgr, _period)
    return db


def test_dbUserActivity_isSavedByPeriod(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    user_id = "335972576"
    expected_comm_id = tw_mgr.user_community_map[user_id]
    actual_comm_id, actual_user_activity = db.user_summary_by_period(user_id, _period)
    assert actual_comm_id == expected_comm_id
    assert actual_user_activity.tweet_count == 2
    assert actual_user_activity.retweeted_count == 2
    assert actual_user_activity.replied_to_count == 1

    for user_id, expected_user_activity in tw_mgr.user_activity.items():
        expected_comm_id = tw_mgr.user_community_map[user_id]
        actual_comm_id, actual_user_activity = db.user_summary_by_period(user_id, _period)
        assert actual_comm_id == expected_comm_id
        assert actual_user_activity.tweet_count == expected_user_activity.tweet_count
        assert actual_user_activity.retweet_count == expected_user_activity.retweet_count
        assert actual_user_activity.retweeted_count == expected_user_activity.retweeted_count
        assert actual_user_activity.reply_count == expected_user_activity.reply_count
        assert actual_user_activity.replied_to_count == expected_user_activity.replied_to_count
        assert actual_user_activity.influence == expected_user_activity.influence
        assert actual_user_activity.influence_rank == expected_user_activity.influence_rank


def test_dbCommunityNumTweets_isAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for community_id, activity in community_activity_map.items():
        expected_tweet_count = activity.num_tweets
        community_summary = db.community_summary(community_id, _period)
        actual_tweet_count = community_summary.num_tweets
        assert actual_tweet_count == expected_tweet_count


def test_dbCommunitySentimentScores_areAccurate(get_db, get_communities):
    """verifies SupportsBLM and Sentiment attributes of Community table"""
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for community_id in community_activity_map:
        expected_sentiment = random()
        db.save_community_sentiment(_period, community_id, expected_sentiment)
        comm_summary = db.community_summary(community_id, _period)
        actual_sentiment = comm_summary.sentiment
        assert actual_sentiment == expected_sentiment


def test_dbCommunitySupportsBlmFlags_areAccurate(get_db, get_communities):
    """verifies SupportsBLM and Sentiment attributes of Community table"""
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for row_num, community_id in enumerate(community_activity_map):
        expected_support = row_num < 1
        db.save_community_support(_period, community_id, expected_support)
        comm_summary = db.community_summary(community_id, _period)
        actual_support = comm_summary.supports_blm
        assert actual_support == expected_support


def test_dbCommunityMemeCounts_areAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for community_id, activity in community_activity_map.items():
        expected_meme_counter = activity.meme_counter
        community_summary = db.community_summary(community_id, _period)
        actual_meme_counter = community_summary.meme_counter
        assert actual_meme_counter == expected_meme_counter


def test_dbCommunityRetweetCounts_areAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for community_id, activity in community_activity_map.items():
        expected_retweet_counter = activity.retweet_counter
        community_summary = db.community_summary(community_id, _period)
        actual_retweet_counter = community_summary.retweet_counter
        assert actual_retweet_counter == expected_retweet_counter


def test_dbInterCommunityRetweetCounts_areAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    expected_retweet_counts = inter_comm_retweets
    actual_retweet_counts = db.inter_community_retweet_counts_by_period(_period)
    for k in expected_retweet_counts:
        assert actual_retweet_counts[k] == expected_retweet_counts[k]


def test_dbInterCommunityReplyCounts_areAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    expected_reply_counts = inter_comm_replies
    actual_reply_counts = db.inter_community_reply_counts_by_period(_period)
    for k in expected_reply_counts:
        assert actual_reply_counts[k] == expected_reply_counts[k]


def test_dbCommunitiesSummary_isAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    expected_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    actual_map: Dict[int, CommunityActivity] = db.communities_summary_by_period(_period)
    for expected_id, expected_activity in expected_map.items():
        actual_activity = actual_map[expected_id]
        assert actual_activity.num_tweets == expected_activity.num_tweets
        assert actual_activity.meme_counter == expected_activity.meme_counter
        assert actual_activity.retweet_counter == actual_activity.retweet_counter
        # TODO: test activity.supports_blm and activity.sentiment
