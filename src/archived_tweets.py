import json
import tweepy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import bulk

def trim_tweet(tweet):
    """Remove unused data from tweet """
    tweet["id"] = int(tweet["id_str"])
    tweet.pop("id_str")
    tweet.pop("truncated")
    if "in_reply_to_status_id_str" in tweet:
        if not "in_reply_to_status_id" in tweet:
            tweet["in_reply_to_status_id"] = int(tweet["in_reply_to_status_id_str"])
        tweet.pop("in_reply_to_status_id_str")
    if "in_reply_to_user_id_str" in tweet:
        if not "in_reply_to_user_id" in tweet:
            tweet["in_reply_to_user_id"] = int(tweet["in_reply_to_user_id_str"])
        tweet.pop("in_reply_to_user_id_str")
    if "in_reply_to_screen_name" in tweet:
        tweet.pop("in_reply_to_screen_name")
    if "contributors" in tweet:
        tweet.pop("contributors")
    if "is_quote_status" in tweet:
        tweet.pop("is_quote_status")
    tweet.pop("favorited")
    tweet.pop("retweeted")
    if "extended_entities" in tweet:
        tweet.pop("extended_entities")
    if "possibly_sensitive" in tweet:
        tweet.pop("possibly_sensitive")

    tweet["user_id"] = int(tweet["user"]["id_str"])
    if "user_id_str" in tweet:
        tweet.pop("user_id_str")
    tweet.pop("user")

    is_retweet = "retweeted_status" in tweet
    tweet["is_retweet"] = is_retweet

    entities_dict = tweet["entities"]
    if is_retweet:
        tweet["source_status_id"] = int(tweet["retweeted_status"]["id_str"])
        tweet["source_user_id"] = int(tweet["retweeted_status"]["user"]["id_str"])
    tweet["hashtags"] = entities_dict["hashtags"]
    if "user_mentions" in entities_dict:
        tweet["user_mentions"] = entities_dict["user_mentions"]
    if "media" in entities_dict:
        tweet["media_url"] = entities_dict["media"][0]["media_url"]

    tweet.pop("entities")
    return tweet

def get_action(tweet):
    return {
        "_index": "tweet",
        "_id": tweet["id"],
        "doc": tweet
    }

print("starting")

api_key = 'FsVHLxN3iI4wVbtMWSI3ehjh9'
api_secret = 'iNnp9KjFMDalWwWoAcH96DBx6F7jZzpHvuQHOuMSdAkMQndgpZ'

auth_handler = tweepy.AppAuthHandler(api_key,api_secret)
api = tweepy.API(auth_handler, wait_on_rate_limit=True)

es = ES('localhost')

tweet_id_file = "./data/olteanu_ids.txt"
if __name__ == '__main__':
    with open(tweet_id_file, 'r') as f:
        stream = True
        while (stream):
            tweet_ids = []
            for i in range(10):
                id_ = f.readline()
                if not id_:
                    break
                tweet_ids.append(int(id_))
            if tweet_ids[-1] <= 597189127243575296: #last successful ID before connection error
                continue
            statuses = api.statuses_lookup(tweet_ids, trim_user=True, tweet_mode = "compat")

            es_actions = []
            for status in statuses:
                tweet = status._json
                tweet = trim_tweet(tweet)
                if "retweeted_status" in tweet:
                    retweet = tweet["retweeted_status"]
                    retweet = trim_tweet(retweet)
                    es_actions.append(get_action(retweet))
                    tweet.pop("retweeted_status")
                es_actions.append(get_action(tweet))

            bulk(es, es_actions)
            print(f"last id: {tweet_ids[-1]}")
