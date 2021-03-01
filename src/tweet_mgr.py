from collections import Counter, defaultdict

import leidenalg as la

from graph_mgr import UserRetweetGraph

class TweetsManager():

    def __init__(self):
        self.tweets = dict()                          # tweet_id -> tweet_text
        self.user_tweet_counter = Counter()           # user_id -> num tweets
        self.user_meme_counter = defaultdict(Counter) # user_id -> meme counter TODO: delete this?
        self.user_retweeted_frequency = Counter()     # user_id -> num times retweeted
        self.urg = UserRetweetGraph()                 # to identify communities
        self.retweet_meme_counter = defaultdict(Counter) # (retweeter_id, retweeted_id) -> meme counter
        self.retweets = defaultdict(list)             # (retweeter_id, retweeted_id) -> list of tweet_id
        self.reply_counter = Counter()                # (replying_user, replied_to_user) -> tweet_count

    def process_tweet(self, tweet):
        doc = tweet["doc"]
        self.tweets[doc["id"]] = doc["text"]
 
        user_id = str(doc["user_id"])
        self.user_tweet_counter[user_id] += 1

        memes = [d["text"].lower() for d in doc["hashtags"]]
        memes = [m for m in memes if m not in ('blacklivesmatter', 'blm')]
        for meme in memes:
            self.user_meme_counter[user_id][meme] += 1

        if doc["is_retweet"]:
            retweet_id = doc["source_status_id"]
            retweeted_user_id = str(doc["source_user_id"])
            self.user_retweeted_frequency[retweeted_user_id] += 1
            self.urg.add_retweet(user_id, retweeted_user_id)
            for meme in memes:
                self.retweet_meme_counter[(user_id, retweeted_user_id)][meme] += 1
            self.retweets[(user_id, retweeted_user_id)].append(retweet_id)
        else: 
            reply_to_user = doc["in_reply_to_user_id"]
            if reply_to_user:
                self.reply_counter[(user_id, str(reply_to_user))] += 1

    def analyze_communities(self, resolution_parameter = 1.0, n_iterations = 2):
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
            la.CPMVertexPartition, # TODO - set this to ModularityVertexPartition, change other params accordingly
            resolution_parameter=resolution_parameter,
            n_iterations=n_iterations
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
            tweeted_id = user_pair[1]
            tweeter_community = self.user_community_map[tweeter_id]
            tweeted_community = self.user_community_map[tweeted_id]
            if tweeter_community == tweeted_community:
                comm = tweeter_community
                for retweet_id in self.retweets[user_pair]:
                    self.community_retweet_counter[comm][retweet_id] += 1
                pair_meme_counter = self.retweet_meme_counter[(tweeter_id, tweeted_id)]
                for meme in pair_meme_counter:
                    self.community_meme_counter[comm][meme] += pair_meme_counter[meme]
            else:
                self.inter_comm_retweet_counter[(tweeter_community, tweeted_community)] += 1
        for user_pair in self.reply_counter:
            # the reply might be to a tweet that didn't include BLM, so the user might not be in the dataset
            replying_comm = self.user_community_map.get(user_pair[0])
            reply_to_comm = self.user_community_map.get(user_pair[1])
            if replying_comm and reply_to_comm and replying_comm != reply_to_comm:
                self.inter_comm_reply_counter[(replying_comm, reply_to_comm)] += self.reply_counter[user_pair]
        
        for cid in self.community_user_map:
            num_tw = sum([self.user_tweet_counter[uid] for uid in self.community_user_map[cid]])
            self.comm_tweet_counter[cid] = num_tw

                