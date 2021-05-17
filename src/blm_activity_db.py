from typing import Dict

import sqlite3 as sql

from src.tweet_mgr import TweetsManager, UserActivity

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
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(_account_table)
            cur.execute(_community_table)
            cur.execute(_account_activity_table)
            cur.execute(_retweet_table)
            cur.execute(_reply_table)


    def save_tweets_mgr(self, tw_mgr: TweetsManager, period_no: int):
        self._save_accounts(tw_mgr.user_activity)
        self._save_communities(tw_mgr.community_user_map, period_no)
        self._save_user_activity(tw_mgr.user_activity, tw_mgr.user_community_map, period_no)


    def _save_accounts(self, user_activity_map):
        account_insert = "INSERT or IGNORE into Account(AccountId) values (?)"
        with self.conn:
            cur = self.conn.cursor()
            for user_id in user_activity_map:
                cur.execute(account_insert, (user_id,))


    def _save_communities(self, community_user_map, period_no):
        community_insert = "INSERT into Community(PeriodId, CommunityId) values (?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for community_id in community_user_map:
                cur.execute(community_insert, (period_no, community_id))


    def _save_user_activity(self, user_activity_map: Dict[str, UserActivity], user_community_map, period_no):
        account_activity_insert = "INSERT into AccountActivity Values(?, ?, ?, ?, ?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for user_id, user_activity in user_activity_map.items():
                comm_id = user_community_map[user_id]
                vals = (
                    user_id,
                    period_no,
                    comm_id,
                    user_activity.tweet_count,
                    user_activity.retweet_count,
                    user_activity.retweeted_count,
                    user_activity.reply_count,
                    user_activity.replied_to_count
                )
                cur.execute(account_activity_insert, vals)
        

    def communities_summary_by_period(self, period_no):
        pass


    def user_summary_by_period(self, user_id, period_no):
        pass


    def user_summary(self, user_id):
        pass
