from numpy.testing import assert_array_equal
import pandas as pd
import pytest
from random import random
import sys
sys.path.append("src")
sys.path.append("tests")
from typing import Dict

from src.blm_activity_db import BlmActivityDb
from src.tweet_sentiment import SentimentAnalysis, summarize_sentiment
from src.tweet_mgr import (
    AccountReply,
    AccountRetweet,
    CommunityActivity, 
    CommunityReply,
    CommunityRetweet,
    Stance,
    TweetsManager,
    UserActivity
)
from tests.test_tweet_mgr import get_communities, get_tw_mgr


_period = 1


# These inter-community activity dictionaries are logically inconsistent with individual
# tweets. They are fabricated simply to test inter-community activity tables.
inter_comm_retweets = {
    (AccountRetweet("335972576", "2378713202"), CommunityRetweet(0, 1)): 4, 
    (AccountRetweet("2378713202", "335972576"), CommunityRetweet(1, 0)): 3
}
inter_comm_replies = {
    (AccountReply("335972576", "2378713202"), CommunityReply(0, 1)): 8, 
    (AccountReply("2378713202", "335972576"), CommunityReply(1, 0)): 7
}

@pytest.fixture
def get_db(get_communities) -> BlmActivityDb:
    db = BlmActivityDb(":memory:", initialize_db=True)
    tw_mgr = get_communities
    tw_mgr.inter_comm_retweet_counter = inter_comm_retweets
    tw_mgr.inter_comm_reply_counter = inter_comm_replies
    db.save_tweets_mgr(tw_mgr, _period)
    return db


def sentiment_analyses_are_equal(sa1: SentimentAnalysis, sa2: SentimentAnalysis) -> bool:
     return sa1.sentiment == sa2.sentiment \
        and sa1.pronoun_counts == sa2.pronoun_counts \
        and sa1.emo_scores == sa2.emo_scores

def test_dbUserActivity_isSavedByPeriod(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr: TweetsManager = get_communities
    user_id = "335972576"
    expected_comm_id = tw_mgr.user_community_map[user_id]
    actual_comm_id, actual_activity = db.user_summary_by_period(user_id, _period)
    assert actual_comm_id == expected_comm_id
    assert actual_activity.tweet_count == 2
    assert actual_activity.retweeted_count == 2
    assert actual_activity.replied_to_count == 1

    for user_id, expected_activity in tw_mgr.user_activity.items():
        expected_comm_id = tw_mgr.user_community_map[user_id]
        actual_comm_id, actual_activity = db.user_summary_by_period(user_id, _period)
        assert actual_comm_id == expected_comm_id
        assert actual_activity.tweet_count == expected_activity.tweet_count
        assert actual_activity.retweet_count == expected_activity.retweet_count
        assert actual_activity.retweeted_count == expected_activity.retweeted_count
        assert actual_activity.reply_count == expected_activity.reply_count
        assert actual_activity.replied_to_count == expected_activity.replied_to_count
        assert actual_activity.influence == expected_activity.influence
        assert actual_activity.influence_rank == expected_activity.influence_rank
        expected_sentiment = summarize_sentiment(expected_activity.sentiment_analyses)
        assert sentiment_analyses_are_equal(actual_activity.sentiment_summary, expected_sentiment)


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
    for community_id, activity in community_activity_map.items():
        comm_summary = db.community_summary(community_id, _period)
        actual_sentiment_all = comm_summary.all_sentiment_summary
        expected_sentiment_all = summarize_sentiment(activity.all_sentiment_analyses)
        assert sentiment_analyses_are_equal(actual_sentiment_all, expected_sentiment_all)
        actual_sentiment_retweets = comm_summary.retweet_sentiment_summary
        expected_sentiment_retweets = summarize_sentiment(activity.retweet_sentiment_analyses)
        assert sentiment_analyses_are_equal(actual_sentiment_retweets, expected_sentiment_retweets)


def test_dbCommunityStanceFlags_areAccurate(get_db, get_communities):
    """verifies SupportsBLM and Sentiment attributes of Community table"""
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for row_num, community_id in enumerate(community_activity_map):
        expected_stance = Stance.CounterProtest if row_num < 1 else Stance.Protest
        db.save_community_support(_period, community_id, expected_stance)
        comm_summary = db.community_summary(community_id, _period)
        actual_stance = comm_summary.stance
        assert actual_stance == expected_stance


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
    actual_df = db.inter_community_retweet_counts_by_period(_period)
    assert isinstance(actual_df, pd.DataFrame)
    expected = []
    for (ar, cr), count in inter_comm_retweets.items():
        expected.append([ar[0], ar[1], cr[0], cr[1], count])
    expected_df = pd.DataFrame(expected, columns=list(actual_df.columns))
    # sort order is not guaranteed, so sort both dataframes to align them
    actual_df.sort_values(by="RetweetingComm", inplace=True)
    expected_df.sort_values(by="RetweetingComm", inplace=True)
    assert_array_equal(actual_df.values, expected_df.values)


def test_dbInterCommunityReplyCounts_areAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    actual_df = db.inter_community_reply_counts_by_period(_period)
    assert isinstance(actual_df, pd.DataFrame)
    expected = []
    for (ar, cr), count in inter_comm_replies.items():
        expected.append([ar[0], ar[1], cr[0], cr[1], count])
    expected_df = pd.DataFrame(expected, columns=list(actual_df.columns))
    # sort order is not guaranteed, so sort both dataframes to align them
    actual_df.sort_values(by="ReplyingComm", inplace=True)
    expected_df.sort_values(by="ReplyingComm", inplace=True)
    assert_array_equal(actual_df.values, expected_df.values)


def test_dbCommunitiesSummary_isAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    expected_map: Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    actual_map: Dict[int, CommunityActivity] = db.communities_summary_by_period(_period)
    for expected_id, expected_activity in expected_map.items():
        actual_activity = actual_map[expected_id]
        assert actual_activity.num_tweets == expected_activity.num_tweets
        assert actual_activity.meme_counter == expected_activity.meme_counter
        assert actual_activity.retweet_counter == expected_activity.retweet_counter
        expected_sa = summarize_sentiment(expected_activity.all_sentiment_analyses)
        assert sentiment_analyses_are_equal(actual_activity.all_sentiment_summary, expected_sa)
        expected_sa = summarize_sentiment(expected_activity.retweet_sentiment_analyses)
        assert sentiment_analyses_are_equal(actual_activity.retweet_sentiment_summary, expected_sa)
        # TODO: test activity.stance


def test_getAccountList_shouldReturnAllAccounts_whenDefaultsApply(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    expected_list = [account for account in tw_mgr.user_activity]
    actual_list = db.get_account_list()
    assert set(actual_list) == set(expected_list)


def test_getAccountList_shouldReturnEmptyList_whenNoAccountsInPeriodRange(get_db):
    db: BlmActivityDb = get_db
    actual_list = db.get_account_list(start_period = _period + 1)
    assert actual_list == []
