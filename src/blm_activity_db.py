from collections import Counter, defaultdict
from enum import IntEnum
from typing import Dict, Tuple

import sqlite3 as sql

from src.tweet_mgr import CommunityActivity, TweetsManager, UserActivity

_account_table = \
"""CREATE TABLE IF NOT EXISTS Account (
    AccountId TEXT PRIMARY KEY
) without RowId"""

_community_table = \
"""CREATE TABLE IF NOT EXISTS Community (
    PeriodId INT,
    CommunityId INT,
    SupportsBlm INT,
    Sentiment REAL,
    NumTweets INT,
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
    Influence REAL,
    InfluenceRank INT,
    PRIMARY KEY (AccountId, PeriodId),
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

_community_meme_table = \
"""CREATE TABLE IF NOT EXISTS CommunityMeme (
    PeriodId INT,
    CommunityId INT,
    Meme TEXT,
    Count INT,
    PRIMARY KEY (PeriodId, CommunityId, Meme),
    FOREIGN KEY (PeriodId, CommunityId) REFERENCES Community (PeriodId, CommunityId)
) without RowId"""

_community_retweet_table = \
"""CREATE TABLE IF NOT EXISTS CommunityRetweet (
    PeriodId INT,
    CommunityId INT,
    TweetId INT,
    NumRetweets INT,
    PRIMARY KEY (PeriodId, CommunityId, TweetId),
    FOREIGN KEY (PeriodId, CommunityId) REFERENCES Community (PeriodId, CommunityId)
) without RowId"""

class AccountActivity(IntEnum):
    AccountId = 0
    PeriodId = 1
    CommunityId = 2
    NumTweets = 3
    NumRetweets = 4
    NumRetweeted = 5
    NumReplies = 6
    NumRepliedTo = 7
    Influence = 8
    InfluenceRank = 9


class Community(IntEnum):
    PeriodId = 0
    CommunityId = 1
    SupportsBlm = 2
    Sentiment = 3
    NumTweets = 4


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
            cur.execute(_community_meme_table)
            cur.execute(_community_retweet_table)


    def save_tweets_mgr(self, tw_mgr: TweetsManager, period_no: int):
        self._save_accounts(tw_mgr.user_activity)
        self._save_communities(tw_mgr.community_activity_map, period_no)
        self._save_user_activity(tw_mgr.user_activity, tw_mgr.user_community_map, period_no)
        self._save_community_activity(tw_mgr.community_activity_map, period_no)


    def _save_accounts(self, user_activity_map):
        account_insert = "INSERT or IGNORE into Account(AccountId) values (?)"
        with self.conn:
            cur = self.conn.cursor()
            for user_id in user_activity_map:
                cur.execute(account_insert, (user_id,))


    def _save_communities(self, community_activity_map, period_no):
        community_insert = "INSERT into Community(PeriodId, CommunityId, NumTweets) values (?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for community_id, community_activity in community_activity_map.items():
                num_tweets = community_activity.num_tweets
                cur.execute(community_insert, (period_no, community_id, num_tweets))


    def _save_user_activity(self, user_activity_map: Dict[str, UserActivity], user_community_map, period_no):
        account_activity_insert = "INSERT into AccountActivity Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for user_id, user_activity in user_activity_map.items():
                comm_id = user_community_map[user_id]
                params = (
                    user_id,
                    period_no,
                    comm_id,
                    user_activity.tweet_count,
                    user_activity.retweet_count,
                    user_activity.retweeted_count,
                    user_activity.reply_count,
                    user_activity.replied_to_count,
                    user_activity.influence,
                    user_activity.influence_rank
                )
                cur.execute(account_activity_insert, params)


    def _save_community_activity(self, comm_activity_map, period_no):
        comm_meme_insert = "INSERT into CommunityMeme Values(?, ?, ?, ?)"
        comm_retweet_insert = "INSERT into CommunityRetweet Values(?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for community_id, comm_activity in comm_activity_map.items():
                for meme, count in comm_activity.meme_counter.items():
                    params = (period_no, community_id, meme, count)
                    cur.execute(comm_meme_insert, params)
                for tweet_id, num_retweets in comm_activity.retweet_counter.items():
                    params = (period_no, community_id, tweet_id, num_retweets)
                    cur.execute(comm_retweet_insert, params)

    def save_community_sentiment(self, period_no, community_id, sentiment):
        update = "UPDATE Community SET Sentiment = ? where PeriodId = ? and CommunityId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(update, (sentiment, period_no, community_id))


    def save_community_support(self, period_no, community_id, supports_blm):
        update = "UPDATE Community SET SupportsBlm = ? where PeriodId = ? and CommunityId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(update, (supports_blm, period_no, community_id))


    def user_summary_by_period(self, user_id: str, period_no: int) -> Tuple[int, UserActivity]:
        """returns a tuple of community_id, user_activity for a given user in a given period"""
        query = "SELECT * from AccountActivity where AccountId = ? and PeriodId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(query, (user_id, period_no))
            r = cur.fetchone()
        community_id = r[AccountActivity.CommunityId]
        user_activity = UserActivity()
        user_activity.tweet_count = r[AccountActivity.NumTweets]
        user_activity.retweet_count = r[AccountActivity.NumRetweets]
        user_activity.retweeted_count = r[AccountActivity.NumRetweeted]
        user_activity.reply_count = r[AccountActivity.NumReplies]
        user_activity.replied_to_count = r[AccountActivity.NumRepliedTo]
        user_activity.influence = r[AccountActivity.Influence]
        user_activity.influence_rank = r[AccountActivity.InfluenceRank]
        return community_id, user_activity


    def user_summary(self, user_id):
        pass


    def community_summary(self, community_id: int, period_no: int) -> CommunityActivity:
        community_query = "SELECT * from Community where PeriodId = ? and CommunityId = ?"
        meme_query = "SELECT Meme, Count from CommunityMeme where PeriodId = ? and CommunityId = ?"
        retweet_query = "SELECT TweetId, NumRetweets from CommunityRetweet where PeriodId = ? and CommunityId = ?"
        params = (period_no, community_id)
        community_activity = CommunityActivity()
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(community_query, params)
            comm_row = cur.fetchone()
            cur.execute(meme_query, params)
            meme_rows = cur.fetchall()
            cur.execute(retweet_query, params)
            retweet_rows = cur.fetchall()
        # community
        community_activity.num_tweets = comm_row[Community.NumTweets]
        community_activity.sentiment = comm_row[Community.Sentiment]
        community_activity.supports_blm = comm_row[Community.SupportsBlm]
        # memes
        meme_pos, count_pos = 0, 1
        for row in meme_rows:
            community_activity.meme_counter[row[meme_pos]] = row[count_pos]
        # retweets
        tweet_id_pos, num_retweets_pos = 0, 1
        for row in retweet_rows:
            community_activity.retweet_counter[row[tweet_id_pos]] = row[num_retweets_pos]
        return community_activity


    def communities_summary_by_period(self, period_no: int) -> Dict[int, CommunityActivity]:
        """Returns a map of community_id -> CommunityActivity for the requested period"""
        comm_query = "SELECT CommunityId, SupportsBlm, Sentiment, NumTweets from Community where PeriodId = ?"
        meme_query = "SELECT CommunityId, Meme, Count from CommunityMeme where PeriodId = ?"
        retweet_query = "SELECT CommunityId, TweetId, NumRetweets from CommunityRetweet where PeriodId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(comm_query, (period_no,))
            comm_rows = cur.fetchall()
            cur.execute(meme_query, (period_no,))
            meme_rows = cur.fetchall()
            cur.execute(retweet_query, (period_no,))
            retweet_rows = cur.fetchall()
        summary = defaultdict(CommunityActivity)
        # community
        id_pos, supports_blm_pos, sentiment_pos, num_tweets_pos = 0, 1, 2, 3
        for row in comm_rows:
            activity = summary[row[id_pos]]
            activity.num_tweets = row[num_tweets_pos] 
            activity.supports_blm = row[supports_blm_pos]
            activity.sentiment = row[sentiment_pos]
        # memes
        id_pos, meme_pos, count_pos = 0, 1, 2
        for row in meme_rows:
            activity = summary[row[id_pos]]
            activity.meme_counter[row[meme_pos]] = row[count_pos]
        # retweets
        id_pos, tweet_id_pos, count_pos = 0, 1, 2
        for row in retweet_rows:
            activity = summary[row[id_pos]]
            activity.retweet_counter[row[tweet_id_pos]] = row[count_pos]
        return summary
