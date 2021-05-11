from collections import Counter, defaultdict

import leidenalg as la

from graph_mgr import UserRetweetGraph


class DeferredRetweet():

    def __init__(self, user_id, retweeted_user, retweeted_id, memes):
        self.user_id = user_id
        self.retweeted_user = retweeted_user
        self.retweeted_id = retweeted_id
        self.memes = memes


class DeferredReplyTo():
    def __init__(self, user_id, to_user_id, memes):
        self.user_id = user_id
        self.to_user_id = to_user_id
        self.memes = memes


class UserActivity():

    def __init__(self):
        self.tweet_count = 0
        self.retweet_count = 0
        self.retweeted_count = 0
        self.reply_count = 0
        self.replied_to_count = 0
        self.meme_counter = Counter()      # meme -> count


# TODO: write unit test(s) to make sure communities are partitioned as expected
class TweetsManager():


    def __init__(self):
        self.urg = UserRetweetGraph()                 # to identify communities
        self.tweets = dict()                          # tweet_id -> tweet_text
        self.user_activity = defaultdict(UserActivity)   # user_id -> UserActivity
        self.retweet_meme_counter = defaultdict(Counter) # (retweeter_id, retweeted_id) -> meme counter
        self.retweets = defaultdict(list)             # (retweeter_id, retweeted_id) -> list of tweet_id
        self.reply_counter = Counter()                # (replying_user, replied_to_user) -> tweet_count
        self.deferred_retweets = []
        self.deferred_reply_tos = []


    def process_tweet(self, tweet):
        doc = tweet["doc"]
        self.tweets[doc["id"]] = doc["text"]
 
        user_id = str(doc["user_id"])
        self.user_activity[user_id].tweet_count += 1

        memes = [d["text"].lower() for d in doc["hashtags"]]
        memes = [m for m in memes if m not in ('blacklivesmatter', 'blm')]
        for meme in memes:
            self.user_activity[user_id].meme_counter[meme] += 1

        if doc["is_retweet"]:
            retweet_id = int(doc["source_status_id"])
            retweeted_user_id = str(doc["source_user_id"])
            if retweet_id not in self.tweets:
                deferred_retweet = DeferredRetweet(user_id, retweeted_user_id, retweet_id, memes)
                self.deferred_retweets.append(deferred_retweet)
            else:
                self._process_retweet(user_id, retweeted_user_id, retweet_id, memes)
        else: 
            reply_to_user = doc["in_reply_to_user_id"]
            if reply_to_user:
                reply_to_user = str(reply_to_user)
                if reply_to_user not in self.user_activity:
                    self.deferred_reply_tos.append(DeferredReplyTo(user_id, reply_to_user, memes))
                else:
                    self._process_reply(user_id, reply_to_user)


    def _process_retweet(self, user_id, retweeted_user_id, retweet_id, memes):
        self.user_activity[user_id].retweet_count += 1
        self.user_activity[retweeted_user_id].retweeted_count += 1
        self.urg.add_retweet(user_id, retweeted_user_id)
        for meme in memes:
            self.retweet_meme_counter[(user_id, retweeted_user_id)][meme] += 1
        self.retweets[(user_id, retweeted_user_id)].append(retweet_id)


    def _process_reply(self, user_id, reply_to_user):
        self.reply_counter[(user_id, reply_to_user)] += 1
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
                self._process_reply(reply.user_id, reply.to_user_id)
            else:
                self.user_activity[reply.user_id].reply_count += 1


    def analyze_communities(self, n_iterations = 5):
        self.community_user_map = {}                       # community_id -> list of user_id
        self.user_community_map = {}                       # user_id -> community_id
        self.community_meme_counter = defaultdict(Counter)  # community_id -> meme counter
        self.community_retweet_counter = defaultdict(Counter) # community_id -> tweet_id counter
        self.inter_comm_retweet_counter = Counter()        # (retweeter_comm, retweeted_comm) -> retweet counter
        self.inter_comm_reply_counter = Counter()          # (replying_comm, replied_to_comm) -> reply counter
        self.comm_tweet_counter = Counter()                # community_id -> num tweets

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
            self.community_user_map[community_id] = user_ids
        
        for user_pair in self.retweets:
            tweeter_id = user_pair[0]
            retweeted_id = user_pair[1]
            tweeter_community = self.user_community_map[tweeter_id]
            tweeted_community = self.user_community_map[retweeted_id]
            if tweeter_community is None or tweeted_community is None:
                continue
            if tweeter_community == tweeted_community:
                comm = tweeter_community
                for retweet_id in self.retweets[user_pair]:
                    self.community_retweet_counter[comm][retweet_id] += 1
                pair_meme_counter = self.retweet_meme_counter[(tweeter_id, retweeted_id)]
                for meme in pair_meme_counter:
                    self.community_meme_counter[comm][meme] += pair_meme_counter[meme]
            else:
                self.inter_comm_retweet_counter[(tweeter_community, tweeted_community)] += 1
        
        for user_pair in self.reply_counter:
            replying_comm = self.user_community_map.get(user_pair[0])
            reply_to_comm = self.user_community_map.get(user_pair[1])
            # the reply might be to a tweet that didn't include BLM, so the user might not be in the dataset
            if replying_comm is None or reply_to_comm is None:
                continue
            if replying_comm != reply_to_comm:
                self.inter_comm_reply_counter[(replying_comm, reply_to_comm)] += self.reply_counter[user_pair]
        
        for cid in self.community_user_map:
            num_tw = sum(
                self.user_activity[uid].tweet_count 
                for uid in self.community_user_map[cid]
                )
            self.comm_tweet_counter[cid] = num_tw
               