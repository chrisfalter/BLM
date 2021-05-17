import pytest
import sys
sys.path.append("src")
sys.path.append("tests")

from src.blm_activity_db import BlmActivityDb
from src.tweet_mgr import UserActivity
from tests.test_tweet_mgr import get_communities, get_tw_mgr


@pytest.fixture
def get_db() -> BlmActivityDb:
    db = BlmActivityDb(":memory:", initialize_db=True)
    return db


def test_userActivity_isSaved_byPeriod(get_db, get_communities):
    period = 0
    tw_mgr = get_communities
    db: BlmActivityDb = get_db
    db.save_tweets_mgr(tw_mgr, period)
    user_id = "335972576"
    user_activity: UserActivity = db.user_summary_by_period(user_id, period)
    assert user_activity.tweet_count == 2
    assert user_activity.retweeted_count == 2
    assert user_activity.replied_to_count == 1
    user_id = "247052159"
    user_activity: UserActivity = db.user_summary_by_period(user_id, period)
    assert user_activity.tweet_count == 3
    assert user_activity.retweet_count == 1
    assert user_activity.reply_count == 2
