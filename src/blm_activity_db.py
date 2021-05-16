import sqlite3 as sql

from tweet_mgr import UserActivity

_account_table = \
"""CREATE TABLE IF NOT EXISTS Account (
    AccountId TEXT PRIMARY KEY
) without RowId"""

_community_table = \
"""CREATE TABLE IF NOT EXISTS Community (
    PeriodId INT,
    CommunityId INT,
    BlmSupport INT,
    Sentiment REAL,
    PRIMARY KEY (PeriodId, CommunityId)
) without RowId"""

_account_activity_table = \
"""CREATE TABLE IF NOT EXISTS AccountActivity (
    AccountId TEXT,
    PeriodId INT,
    CommunityId INT,
    NumTweets INT,
    NumRetweets INT,
    NumRetweeted INT,
    NumReplies INT,
    NumRepliedTo INT,
    PRIMARY KEY (AccountId, PeriodId, CommunityId),
    FOREIGN KEY (AccountId) REFERENCES Account (AccountId),
    FOREIGN KEY (PeriodId, CommunityId) REFERENCES Community (PeriodId, CommunityId)
) without RowId"""

_retweet_table = \
"""CREATE TABLE IF NOT EXISTS Retweet (
    RetweetAccountId TEXT,
    SourceAccountId TEXT,
    PeriodId INT,
    Count INT,
    PRIMARY KEY (RetweetAccountId, SourceAccountId, PeriodId),
    FOREIGN KEY (SourceAccountId) REFERENCES Account (AccountId),
    FOREIGN KEY (RetweetAccountId) REFERENCES Account (AccountId)
) without RowId"""

_reply_table = \
"""CREATE TABLE IF NOT EXISTS Reply (
    ReplyAccountId TEXT,
    ToAccountId TEXT,
    PeriodId INT,
    Count INT,
    PRIMARY KEY (ToAccountId, ReplyAccountId, PeriodId),
    FOREIGN KEY (ToAccountId) REFERENCES Account (AccountId),
    FOREIGN KEY (ReplyAccountId) REFERENCES Account (AccountId)
) without RowId"""    


class BlmActivityDb():

    def __init__(self, connection_string="../data/db/blm.db", initialize_db=False):
        self.conn = sql.connect(connection_string)
        if initialize_db:
            self._initialize_db()


    def _initialize_db(self):
        pass


    def save_tweets_mgr(self, tw_mgr, period_no):
        pass


    def communities_summary_by_period(self, period_no):
        pass


    def user_summary_by_period(self, user_id, period_no):
        pass


    def user_summary(self, user_id):
        pass
