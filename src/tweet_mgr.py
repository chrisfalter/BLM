from collections import Counter, defaultdict
from typing import Dict, List, NamedTuple

import leidenalg as la

from graph_mgr import UserRetweetGraph
from tweet_sentiment import SentimentAnalysis, eval_tweet_sentiment


class DeferredRetweet():

    def __init__(self, user_id, retweeted_user, retweeted_id, memes):
        self.user_id = user_id
        self.retweeted_user = retweeted_user
        self.retweeted_id = retweeted_id
        self.memes = memes


class DeferredReplyTo():
    def __init__(self, user_id, to_user_id, memes, tweet_id):
        self.user_id = user_id
        self.to_user_id = to_user_id
        self.memes = memes
        self.tweet_id = tweet_id


class UserActivity():

    def __init__(self):
        self.tweet_count = 0
        self.retweet_count = 0
        self.retweeted_count = 0
        self.reply_count = 0
        self.replied_to_count = 0
        self.influence = 0.0               # PageRank score
        self.influence_rank = 0
        self.meme_counter = Counter()      # meme -> count
        self.sentiment_analyses: List[SentimentAnalysis] = []
        self.sentiment_summary: SentimentAnalysis


class CommunityActivity():

    def __init__(self):
        self.num_tweets = 0
        self.supports_blm = False
        self.retweet_sentiment_analyses: List[SentimentAnalysis] = []
        self.retweet_sentiment_summary: SentimentAnalysis
        self.all_sentiment_analyses: List[SentimentAnalysis] = []
        self.all_sentiment_summary: SentimentAnalysis
        self.meme_counter = Counter() # meme -> count for 
        self.retweet_counter = Counter() # tweet_id ->count


class AccountRetweet(NamedTuple):
    retweeter: str
    retweeted: str


class AccountReply(NamedTuple):
    replying: str
    replied_to: str


class CommunityRetweet(NamedTuple):
    retweeting: int
    retweeted: int


class CommunityReply(NamedTuple):
    replying: int
    replied_to: int


class TweetsManager():


    def __init__(self):
        self.urg = UserRetweetGraph()                 # to identify communities
        self.tweets = dict()                          # tweet_id -> tweet_text
        self.user_activity = defaultdict(UserActivity)   # user_id -> UserActivity
        self.retweet_meme_counter = defaultdict(Counter) # AccountRetweet -> meme counter
        self.retweets = defaultdict(list)                 # AccountRetweet -> list of tweet_id
        self.reply_meme_counter = defaultdict(Counter)   # AccountReply -> meme counter
        self.replies: Dict[AccountReply, List[int]] = defaultdict(list) # AccountReply -> tweets
        self.tweet_sentiment: Dict[str, SentimentAnalysis] = {} # tweet_id -> SentimentAnalysis
        self.deferred_retweets: List[DeferredRetweet] = []
        self.deferred_reply_tos: List[DeferredReplyTo] = []

        self.community_user_map = {}                       # community_id -> list of user_id
        self.user_community_map = {}                       # user_id -> community_id
        self.community_activity_map = defaultdict(CommunityActivity) # community_id -> CommunityActivity
        self.inter_comm_retweet_counter = Counter()        # (AccountRetweet, CommunityRetweet) -> numRetweets
        self.inter_comm_reply_counter = Counter()          # (AccountReply, CommunityReply) -> numReplies



    def process_tweet(self, tweet):
        doc = tweet["doc"]
        self.tweets[doc["id"]] = doc["text"]
 
        user_id = str(doc["user_id"])
        self.user_activity[user_id].tweet_count += 1

        memes = [d["text"].lower() for d in doc["hashtags"]]
        memes = [m for m in memes if m not in ('blacklivesmatter', 'blm')]
        for meme in memes:
            self.user_activity[user_id].meme_counter[meme] += 1

        sa = eval_tweet_sentiment(doc["text"])
        self.user_activity[user_id].sentiment_analyses.append(sa)
        self.tweet_sentiment[doc["id"]] = sa

        if doc["is_retweet"]:
            retweet_id = int(doc["source_status_id"])
            retweeted_user_id = str(doc["source_user_id"])
            if retweet_id not in self.tweets:
                deferred_retweet = DeferredRetweet(user_id, retweeted_user_id, retweet_id, memes)
                self.deferred_retweets.append(deferred_retweet)
            else:
                self._process_retweet(user_id, retweeted_user_id, retweet_id, memes)
        else: 
            reply_to_user = doc.get("in_reply_to_user_id")
            if reply_to_user:
                reply_to_user = str(reply_to_user)
                if reply_to_user not in self.user_activity:
                    self.deferred_reply_tos.append(DeferredReplyTo(user_id, reply_to_user, memes, doc["id"]))
                else:
                    self._process_reply(user_id, reply_to_user, memes, doc["id"])


    def _process_retweet(self, user_id, retweeted_user_id, retweet_id, memes):
        self.user_activity[user_id].retweet_count += 1
        self.user_activity[retweeted_user_id].retweeted_count += 1
        self.urg.add_retweet(user_id, retweeted_user_id)
        for meme in memes:
            self.retweet_meme_counter[(user_id, retweeted_user_id)][meme] += 1
        self.retweets[(user_id, retweeted_user_id)].append(retweet_id)


    def _process_reply(self, user_id, reply_to_user, memes, tweet_id):
        reply_accounts = AccountReply(user_id, reply_to_user)
        self.replies[reply_accounts].append(tweet_id)
        for meme in memes:
            self.reply_meme_counter[reply_accounts][meme] += 1
        self.user_activity[user_id].reply_count += 1
        self.user_activity[reply_to_user].replied_to_count += 1


    def process_deferred_interactions(self):
        """Process deferred replies and retweets to the maximum extent"""
        for retweet in self.deferred_retweets:
            if retweet.retweeted_id in self.tweets:
                self._process_retweet(
                    retweet.user_id, 
                    retweet.retweeted_user, 
                    retweet.retweeted_id,
                    retweet.memes
                    )
            else:
                self.user_activity[retweet.user_id].retweet_count += 1
        for reply in self.deferred_reply_tos:
            if reply.to_user_id in self.user_activity:
                self._process_reply(reply.user_id, reply.to_user_id, reply.memes, reply.tweet_id)
            else:
                self.user_activity[reply.user_id].reply_count += 1


    def analyze_graph(self, n_iterations = 5):
        """Community detection and account influence"""
        self.urg.make_graph()
        self.partition = la.find_partition(
            self.urg.g, 
            la.ModularityVertexPartition, 
            weights="weight",
            n_iterations=n_iterations,
            seed=42
        )
        for i, community in enumerate(self.partition):
            community_id = i
            user_ids = []
            for node_id in community:
                user_id = self.urg.g.vs[node_id]['name']
                self.user_community_map[user_id] = community_id
                user_ids.append(user_id)
                self.community_activity_map[community_id].all_sentiment_analyses += \
                    self.user_activity[user_id].sentiment_analyses
            self.community_user_map[community_id] = user_ids
        
        for user_pair, tweet_ids in self.retweets.items():
            tweeter_id = user_pair[0]
            retweeted_id = user_pair[1]
            tweeter_community = self.user_community_map[tweeter_id]
            tweeted_community = self.user_community_map[retweeted_id]
            if tweeter_community is None or tweeted_community is None:
                continue
            if tweeter_community == tweeted_community:
                comm = tweeter_community
                for retweet_id in tweet_ids:
                    ca = self.community_activity_map[comm]
                    ca.retweet_counter[retweet_id] += 1
                    ca.retweet_sentiment_analyses.append(self.tweet_sentiment[retweet_id])
                pair_meme_counter = self.retweet_meme_counter[(tweeter_id, retweeted_id)]
                for meme in pair_meme_counter:
                    self.community_activity_map[comm].meme_counter[meme] += pair_meme_counter[meme]
            else:
                ar = AccountRetweet(retweeter=tweeter_id, retweeted=retweeted_id)
                cr = CommunityRetweet(tweeter_community, tweeted_community)
                self.inter_comm_retweet_counter[(ar, cr)] += 1
        
        for user_pair in self.replies:
            replying_comm = self.user_community_map.get(user_pair[0])
            reply_to_comm = self.user_community_map.get(user_pair[1])
            # the reply might be to a tweet that didn't include BLM, so the user might not be in the dataset
            if replying_comm is None or reply_to_comm is None:
                continue
            if replying_comm != reply_to_comm:
                ar = AccountReply(replying=user_pair[0], replied_to=user_pair[1])
                cr = CommunityReply(replying=replying_comm, replied_to=reply_to_comm)
                self.inter_comm_reply_counter[(ar, cr)] += len(self.replies[user_pair])
        
        for cid in self.community_user_map:
            num_tw = sum(
                self.user_activity[uid].tweet_count 
                for uid in self.community_user_map[cid]
                )
            self.community_activity_map[cid].num_tweets = num_tw

        # process influence scores
        influence_map = self.urg.get_influence_scores()
        for account, score in influence_map.items():
            self.user_activity[account].influence = score 
        scores = [(account, influence_map[account]) for account in influence_map]  
        sorted_by_influence = sorted(scores, key=lambda t: t[1], reverse=True)
        for i, t in enumerate(sorted_by_influence):
            self.user_activity[t[0]].influence_rank = i + 1 # best rank = 1, not 0
