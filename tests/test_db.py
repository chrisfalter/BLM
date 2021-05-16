import pytest
import sys
sys.path.append("src")
sys.path.append("tests")

from blm_activity_db import BlmActivityDb
from test_tweet_mgr import get_communities, get_tw_mgr


@pytest.fixture
def get_db():
    db = BlmActivityDb(":memory:", initialize_db=True)
    return db


def test_userActivity_isSaved_byPeriod(get_db, get_communities):
    tw_mgr = get_communities

