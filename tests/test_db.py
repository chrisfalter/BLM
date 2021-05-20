import pytest
import sys
sys.path.append("src")
sys.path.append("tests")
from typing import Dict

from src.blm_activity_db import BlmActivityDb
from src.tweet_mgr import CommunityActivity, UserActivity
from tests.test_tweet_mgr import get_communities, get_tw_mgr


_period = 0


@pytest.fixture
def get_db(get_communities) -> BlmActivityDb:
    db = BlmActivityDb(":memory:", initialize_db=True)
    db.save_tweets_mgr(get_communities, _period)
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


def test_dbCommunityNumTweets_isAccurate(get_db, get_communities):
    db: BlmActivityDb = get_db
    tw_mgr = get_communities
    community_activity_map:Dict[int, CommunityActivity] = tw_mgr.community_activity_map
    for community_id, activity in community_activity_map.items():
        expected_tweet_count = activity.num_tweets
        community_summary = db.community_summary(community_id, _period)
        actual_tweet_count = community_summary.num_tweets
        assert actual_tweet_count == expected_tweet_count


def test_dbCommunityMemeCounts_areAccurate(get_db, get_communities):
    pass # TODO: write the test


def test_dbCommunityRetweetCounts_areAccurate(get_db, get_communities):
    pass # TODO: write the test
