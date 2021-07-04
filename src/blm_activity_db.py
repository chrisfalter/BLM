from collections import Counter, defaultdict
from enum import IntEnum
from src.tweet_sentiment import EmoScores, PronounCounts, SentimentAnalysis
import pandas as pd
from typing import Dict, Tuple

import sqlite3 as sql

from tweet_mgr import (
    AccountReply,
    AccountRetweet,
    CommunityReply,
    CommunityRetweet,
    CommunityActivity, 
    TweetsManager, 
    UserActivity,
)
from tweet_sentiment import summarize_sentiment

_account_table = \
"""CREATE TABLE IF NOT EXISTS Account (
    AccountId TEXT PRIMARY KEY
) without RowId"""

_community_table = \
"""CREATE TABLE IF NOT EXISTS Community (
    PeriodId INT,
    CommunityId INT,
    SupportsBlm INT,
    NumTweets INT,
    Sentiment REAL,
    TrustEmo REAL,
    AnticipationEmo REAL,
    JoyEmo REAL,
    SurpriseEmo REAL,
    AngerEmo REAL,
    DisgustEmo REAL,
    FearEmo REAL,
    SadnessEmo REAL,
    FirstSingularUsage REAL,
    FirstPluralUsage REAL,
    SecondUsage REAL,
    ThirdUsage REAL,
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
    Sentiment REAL,
    TrustEmo REAL,
    AnticipationEmo REAL,
    JoyEmo REAL,
    SurpriseEmo REAL,
    AngerEmo REAL,
    DisgustEmo REAL,
    FearEmo REAL,
    SadnessEmo REAL,
    FirstSingularUsage REAL,
    FirstPluralUsage REAL,
    SecondUsage REAL,
    ThirdUsage REAL,
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

_community_retweet_sentiment_table = \
"""CREATE TABLE IF NOT EXISTS CommunityRetweetSentiment (
    PeriodId INT,
    CommunityId INT,
    Sentiment REAL,
    TrustEmo REAL,
    AnticipationEmo REAL,
    JoyEmo REAL,
    SurpriseEmo REAL,
    AngerEmo REAL,
    DisgustEmo REAL,
    FearEmo REAL,
    SadnessEmo REAL,
    FirstSingularUsage REAL,
    FirstPluralUsage REAL,
    SecondUsage REAL,
    ThirdUsage REAL,
    PRIMARY KEY (PeriodId, CommunityId),
    FOREIGN KEY (PeriodId, CommunityId) REFERENCES Community (PeriodId, CommunityId)
) without RowId"""

_inter_community_retweet_table = \
"""CREATE TABLE IF NOT EXISTS InterCommunityRetweet (
    PeriodId INT,
    RetweetingAccountId TEXT,
    RetweetedAccountId TEXT,
    RetweetingCommunityId INT,
    RetweetedCommunityId INT,
    NumRetweets INT,
    PRIMARY KEY (PeriodId, RetweetingCommunityId, RetweetedCommunityId)
) without RowId"""

_inter_community_reply_table = \
"""CREATE TABLE IF NOT EXISTS InterCommunityReply (
    PeriodId INT,
    ReplyingAccountId TEXT,
    RepliedToAccountId TEXT,
    ReplyingCommunityId INT,
    RepliedToCommunityId INT,
    NumRetweets INT,
    PRIMARY KEY (PeriodId, ReplyingCommunityId, RepliedToCommunityId)
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
    Sentiment = 10
    TrustEmo = 11
    AnticipationEmo = 12
    JoyEmo = 13
    SurpriseEmo = 14
    AngerEmo = 15
    DisgustEmo = 16
    FearEmo = 17
    SadnessEmo = 18
    FirstSingularUsage = 19
    FirstPluralUsage = 20
    SecondUsage = 21
    ThirdUsage = 22
 

class Community(IntEnum):
    PeriodId = 0
    CommunityId = 1
    SupportsBlm = 2
    NumTweets = 3
    Sentiment = 4
    TrustEmo = 5
    AnticipationEmo = 6
    JoyEmo = 7
    SurpriseEmo = 8
    AngerEmo = 9
    DisgustEmo = 10
    FearEmo = 11
    SadnessEmo = 12
    FirstSingularUsage = 13
    FirstPluralUsage = 14
    SecondUsage = 15
    ThirdUsage = 16


def sentiment_analysis_from_row(row, sentiment_position: int) -> SentimentAnalysis:
    sentiment = row[sentiment_position]
    emo = EmoScores(
        row[sentiment_position + 1],
        row[sentiment_position + 2],
        row[sentiment_position + 3],
        row[sentiment_position + 4],
        row[sentiment_position + 5],
        row[sentiment_position + 6],
        row[sentiment_position + 7],
        row[sentiment_position + 8]
    )
    pc = PronounCounts(
        row[sentiment_position + 9],
        row[sentiment_position + 10],
        row[sentiment_position + 11],
        row[sentiment_position + 12]
    )
    return SentimentAnalysis(pc, emo, sentiment)


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
            cur.execute(_community_retweet_sentiment_table)
            cur.execute(_inter_community_retweet_table)
            cur.execute(_inter_community_reply_table)


    def save_tweets_mgr(self, tw_mgr: TweetsManager, period_no: int):
        self._save_accounts(tw_mgr.user_activity)
        self._save_communities(tw_mgr.community_activity_map, period_no)
        self._save_user_activity(tw_mgr.user_activity, tw_mgr.user_community_map, period_no)
        self._save_community_activity(tw_mgr.community_activity_map, period_no)
        self._save_inter_community_activity(
            tw_mgr.inter_comm_retweet_counter, 
            tw_mgr.inter_comm_reply_counter, 
            period_no,
        )


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
        insert = "INSERT into AccountActivity Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for user_id, user_activity in user_activity_map.items():
                comm_id = user_community_map[user_id]
                sa = summarize_sentiment(user_activity.sentiment_analyses)
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
                    user_activity.influence_rank,
                    sa.sentiment,
                    sa.emo_scores.trust,
                    sa.emo_scores.anticipation,
                    sa.emo_scores.joy,
                    sa.emo_scores.surprise,
                    sa.emo_scores.anger,
                    sa.emo_scores.disgust,
                    sa.emo_scores.fear,
                    sa.emo_scores.sadness,
                    sa.pronoun_counts.first_singular,
                    sa.pronoun_counts.first_plural,
                    sa.pronoun_counts.second,
                    sa.pronoun_counts.third,
                )
                cur.execute(insert, params)


    def _save_community_activity(self, comm_activity_map: Dict[int, CommunityActivity], period_no):
        meme_insert = "INSERT into CommunityMeme Values(?, ?, ?, ?)"
        retweet_insert = "INSERT into CommunityRetweet Values(?, ?, ?, ?)"
        community_update = \
            "UPDATE Community " \
            "SET Sentiment = ?, " \
                "TrustEmo = ?, " \
                "AnticipationEmo = ?, " \
                "JoyEmo = ?, " \
                "SurpriseEmo = ?, " \
                "AngerEmo = ?, " \
                "DisgustEmo = ?, " \
                "FearEmo = ?, " \
                "SadnessEmo = ?, " \
                "FirstSingularUsage = ?, " \
                "FirstPluralUsage = ?, " \
                "SecondUsage = ?, " \
                "ThirdUsage = ? " \
            "WHERE PeriodId = ? and CommunityId = ?"
        retweet_sentiment_insert = "INSERT into CommunityRetweetSentiment " \
                                   "Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for community_id, comm_activity in comm_activity_map.items():
                for meme, count in comm_activity.meme_counter.items():
                    params = (period_no, community_id, meme, count)
                    cur.execute(meme_insert, params)
                for tweet_id, num_retweets in comm_activity.retweet_counter.items():
                    params = (period_no, community_id, tweet_id, num_retweets)
                    cur.execute(retweet_insert, params)
                csa = summarize_sentiment(comm_activity.all_sentiment_analyses)
                params = (
                    csa.sentiment,
                    csa.emo_scores.trust,
                    csa.emo_scores.anticipation,
                    csa.emo_scores.joy,
                    csa.emo_scores.surprise,
                    csa.emo_scores.anger,
                    csa.emo_scores.disgust,
                    csa.emo_scores.fear,
                    csa.emo_scores.sadness,
                    csa.pronoun_counts.first_singular,
                    csa.pronoun_counts.first_plural,
                    csa.pronoun_counts.second,
                    csa.pronoun_counts.third,
                    period_no,
                    community_id
                )
                cur.execute(community_update, params)
                rsa = summarize_sentiment(comm_activity.retweet_sentiment_analyses)
                params = (
                    period_no,
                    community_id,
                    rsa.sentiment,
                    rsa.emo_scores.trust,
                    rsa.emo_scores.anticipation,
                    rsa.emo_scores.joy,
                    rsa.emo_scores.surprise,
                    rsa.emo_scores.anger,
                    rsa.emo_scores.disgust,
                    rsa.emo_scores.fear,
                    rsa.emo_scores.sadness,
                    rsa.pronoun_counts.first_singular,
                    rsa.pronoun_counts.first_plural,
                    rsa.pronoun_counts.second,
                    rsa.pronoun_counts.third,
                )
                cur.execute(retweet_sentiment_insert, params)


    def _save_inter_community_activity(self, ic_retweets, ic_replies, period_no):
        retweet_insert = "INSERT into InterCommunityRetweet Values(?, ?, ?, ?, ?, ?)"
        reply_insert = "INSERT into InterCommunityReply Values(?, ?, ?, ?, ?, ?)"
        with self.conn:
            cur = self.conn.cursor()
            for (ar, cr), count in ic_retweets.items():
                params = (period_no, ar.retweeter, ar.retweeted, cr.retweeting, cr.retweeted, count)
                cur.execute(retweet_insert, params)
            for (ar, cr), count in ic_replies.items():
                params = (period_no, ar.replying, ar.replied_to, cr.replying, cr.replied_to, count)
                cur.execute(reply_insert, params)


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
        user_activity.sentiment_summary = sentiment_analysis_from_row(r, AccountActivity.Sentiment)
        return community_id, user_activity


    def user_summary(self, user_id):
        pass


    def community_summary(self, community_id: int, period_no: int) -> CommunityActivity:
        community_query = "SELECT * from Community where PeriodId = ? and CommunityId = ?"
        meme_query = "SELECT Meme, Count from CommunityMeme where PeriodId = ? and CommunityId = ?"
        retweet_query = "SELECT TweetId, NumRetweets from CommunityRetweet where PeriodId = ? and CommunityId = ?"
        retweet_sentiment_query = "SELECT * from CommunityRetweetSentiment  where PeriodId = ? and CommunityId = ?"
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
            cur.execute(retweet_sentiment_query, params)
            r_sentiment_row = cur.fetchone()

        # community
        community_activity.num_tweets = comm_row[Community.NumTweets]
        community_activity.supports_blm = comm_row[Community.SupportsBlm]
        community_activity.all_sentiment_summary = sentiment_analysis_from_row(comm_row, Community.Sentiment)

        # memes
        meme_pos, count_pos = 0, 1
        for row in meme_rows:
            community_activity.meme_counter[row[meme_pos]] = row[count_pos]

        # retweets
        tweet_id_pos, num_retweets_pos = 0, 1
        for row in retweet_rows:
            community_activity.retweet_counter[row[tweet_id_pos]] = row[num_retweets_pos]

        # retweet sentiment
        sentiment_pos = 2
        community_activity.retweet_sentiment_summary = sentiment_analysis_from_row(
            r_sentiment_row, 
            sentiment_pos
        )

        return community_activity


    def communities_summary_by_period(self, period_no: int) -> Dict[int, CommunityActivity]:
        """Returns a map of community_id -> CommunityActivity for the requested period"""
        comm_query = "SELECT * from Community where PeriodId = ?"
        meme_query = "SELECT CommunityId, Meme, Count from CommunityMeme where PeriodId = ?"
        retweet_query = "SELECT CommunityId, TweetId, NumRetweets from CommunityRetweet where PeriodId = ?"
        retweet_sentiment_query = "SELECT * from CommunityRetweetSentiment where PeriodId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(comm_query, (period_no,))
            comm_rows = cur.fetchall()
            cur.execute(meme_query, (period_no,))
            meme_rows = cur.fetchall()
            cur.execute(retweet_query, (period_no,))
            retweet_rows = cur.fetchall()
            cur.execute(retweet_sentiment_query, (period_no,))
            retweet_sentiment_rows = cur.fetchall()
        summary = defaultdict(CommunityActivity)

        # community
        id_pos, supports_blm_pos, num_tweets_pos, sentiment_pos = 1, 2, 3, 4
        for row in comm_rows:
            activity = summary[row[id_pos]]
            activity.num_tweets = row[num_tweets_pos] 
            activity.supports_blm = row[supports_blm_pos]
            activity.all_sentiment_summary = sentiment_analysis_from_row(row, sentiment_pos)

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

        # retweet sentiment
        id_pos, sentiment_pos = 1, 2
        for row in retweet_sentiment_rows:
            activity = summary[row[id_pos]]
            activity.retweet_sentiment_summary = sentiment_analysis_from_row(row, sentiment_pos)

        return summary


    def inter_community_retweet_counts_by_period(self, period_no: int) -> pd.DataFrame:
        """Returns DF of inter-community retweets including account info.
        Note: Columns = RetweetingAcct, RetweetedAcct, RetweetingComm, RetweetedComm, Count"""

        query = "SELECT RetweetingAccountId, RetweetedAccountId, RetweetingCommunityId, RetweetedCommunityId, NumRetweets " \
                "FROM InterCommunityRetweet " \
                "WHERE PeriodId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(query, (period_no,))
            retweet_rows = cur.fetchall()
        cols = ["RetweetingAcct", "RetweetedAcct", "RetweetingComm", "RetweetedComm", "Count"]
        return pd.DataFrame(data=retweet_rows, columns=cols)


    def inter_community_reply_counts_by_period(self, period_no: int) -> pd.DataFrame:
        """Returns DF of inter-community retweets including account info.
        Note: Columns = ReplyingAcct, RepliedToAcct, ReplyingComm, RepliedToComm, Count"""

        query = "SELECT ReplyingAccountId, RepliedToAccountId, ReplyingCommunityId, RepliedToCommunityId, NumRetweets " \
                "FROM InterCommunityReply " \
                "WHERE PeriodId = ?"
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(query, (period_no,))
            reply_rows = cur.fetchall()
        cols = ["ReplyingAcct", "RepliedToAcct", "ReplyingComm", "RepliedToComm", "Count"]
        return pd.DataFrame(data=reply_rows, columns=cols)


    def intercomm_account_retweets_by_period(self, period_no: int) -> pd.DataFrame:
        pass


    def intercomm_account_replies_by_period(self, period_no: int) -> pd.DataFrame:
        """Returns DF of inter-community retweets including account info.
        Note: Columns = RetweetingAcct, RetweetedAcct, RetweetingComm, RetweetedComm, Count"""
        pass
