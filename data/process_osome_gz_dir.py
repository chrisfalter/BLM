import glob
import gzip
import json
import os
import sys

from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import bulk

from archived_tweets import trim_tweet

def get_action(tweet):
    return {
        "_index": "tweets2",
        "_id": tweet["id"],
        "doc": tweet
    }


def main(path):
    es = ES('localhost')
    for fname in [fp for fp in glob.glob(os.path.join(path, "**/*.gz"),recursive=True)]:
        es_actions = []
        with gzip.open(fname, 'rt', encoding='utf8') as f:
            for line in f:
                tweet = json.loads(line)
                tweet = trim_tweet(tweet)
                if "retweeted_status" in tweet:
                    retweet = tweet["retweeted_status"]
                    retweet = trim_tweet(retweet)
                    es_actions.append(get_action(retweet))
                    tweet.pop("retweeted_status")
                es_actions.append(get_action(tweet))
        bulk(es, es_actions)

if __name__ == '__main__':
    path = sys.argv[1]
    main(path)



