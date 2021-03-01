# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 1.0.5
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
from   collections import Counter
import json
from   os import path
import pandas as pd
import re
from   string import Template

from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import scan

from community_classifier import get_blm_classifier
from community_report import generate_init_community_report
from tweet_mgr import TweetsManager


# +
# Configurable parameters
start_date = "2014-12-24"
end_date = "2015-05-09"
es_idx = 'tweets'
query_body = {
  "query": {
      "range": {
          "doc.created_at": {
            "gte": "Wed Dec 24 00:00:00 +0000 2014"
          }
      }
  }
}
period = 3
num_init_communities = 25
num_exemplars = 10



# +
tm = TweetsManager()

#query_body = {
#  "query": {
#      "range": {
#          "doc.created_at": {
#            "gte": "Sun Nov 23 00:00:00 +0000 2014",
#            "lt": "Wed Dec 24 00:00:00 +0000 2014"
#          }
#      }
#  }
#}

es = ES(hosts=["localhost"])
# results = es.search(body=query_body, index=es_idx, size=500)
scan_iter = scan(es, index=es_idx, query=query_body)
for result in scan_iter:
    tweet = result['_source']
    tm.process_tweet(tweet)


# -

def get_tweet_text_by_id(id_):
    doc = es.get(index=es_idx, id=id_)
    return doc['_source']['doc']['text']


resolution = 1/(4*len(tm.user_retweeted_frequency))

tm.analyze_communities(resolution_parameter=resolution, n_iterations=-1)

# +
# Get Initial Report
report_dir = f'./Reports/{period}/'
report_name = "Largest Communities Hashtags and Tweets"
report_path = report_dir + f"{report_name}.md"

report = f"# {report_name} in Period {period}\n\n"
# Add section for each of top num_init_communities by membership count
comm_user_counts = sorted(tm.community_user_map.items(), key=lambda x: len(x[1]), reverse=True)
for k, (comm_id, members) in enumerate(comm_user_counts):
    if k == num_init_communities:
        break
    num_members = len(members)
    num_tweets = tm.comm_tweet_counter[comm_id]
    num_retweets = sum(tm.community_retweet_counter[comm_id].values())
    hashtags = []
    ht_counts = []
    meme_counts = sorted(tm.community_meme_counter[comm_id].items(), key=lambda x:x[1], reverse=True)
    for i, (tag, count) in enumerate(meme_counts):
        if i == num_exemplars:
            break
        hashtags.append(tag)
        ht_counts.append(count)
    tweet_ids = []
    rt_counts = []
    retweet_counts = sorted(tm.community_retweet_counter[comm_id].items(), key=lambda x:x[1], reverse=True)
    for i, (tweet_id, count) in enumerate(retweet_counts):
        if i == num_exemplars:
            break
        tweet_ids.append(tweet_id)
        rt_counts.append(count)
    rts = []
    for id_ in tweet_ids:
        if id_ in tm.tweets:
            rts.append(tm.tweets[id_])
        else:
            rts.append(get_tweet_text_by_id(id_))
    report += generate_init_community_report(
        comm_id,
        num_members,
        num_tweets,
        num_retweets,
        hashtags, 
        ht_counts, 
        rts, 
        rt_counts,
)
with open(report_path, 'w', encoding="utf-8") as f:
    f.write(report)
# -

counter_comm_ids = [1]

blm_comm_ids = [i for i in range(num_init_communities)]
for i in counter_comm_ids:
    blm_comm_ids.remove(i)


# +
   
def get_tweet_texts(tweet_ids, tm):
    texts = []
    for id_ in tweet_ids:
        if id_ in tm.tweets:
            texts.append(tm.tweets[id_])
        else:
            texts.append(get_tweet_text_by_id(id_))
    return texts

blm_retweet_set = set()
counter_retweet_set = set()
for k, (comm_id, _ ) in enumerate(comm_user_counts):
    if k == num_init_communities:
        break
    num_exemplars = 288 if comm_id in counter_comm_ids else 18
    retweet_counts = sorted(tm.community_retweet_counter[comm_id].items(), key=lambda x:x[1], reverse=True)
    for i, (tweet_id, _ ) in enumerate(retweet_counts):
        if i == num_exemplars:
            break
        if comm_id in counter_comm_ids:
            counter_retweet_set.add(tweet_id)
        else:
            blm_retweet_set.add(tweet_id)
blm_retweets = get_tweet_texts(blm_retweet_set, tm)
counter_retweets = get_tweet_texts(counter_retweet_set, tm)
blm_clf, cv_results = get_blm_classifier(blm_retweets, counter_retweets)    
# -

results_df = pd.DataFrame(cv_results)
print(results_df)

num_samples_per_comm = 25
unknown_comm_ids = []
for k, (comm_id, _ ) in enumerate(comm_user_counts):
    if k < num_init_communities:
        continue
    sample_tweet_ids = []
    retweet_counts = sorted(tm.community_retweet_counter[comm_id].items(), key=lambda x:x[1], reverse=True)
    for i, (tweet_id, _ ) in enumerate(retweet_counts):
        if i == num_samples_per_comm:
            break
        sample_tweet_ids.append(tweet_id)
    if len(sample_tweet_ids) < num_samples_per_comm:
        unknown_comm_ids.append(comm_id)
        continue
    sample_tweets = get_tweet_texts(sample_tweet_ids, tm)
    num_blm_predictions = sum(blm_clf.predict(sample_tweets))
    if num_blm_predictions < 12:
        counter_comm_ids.append(comm_id)
    elif num_blm_predictions < 14:
        unknown_comm_ids.append(comm_id)
    else:
        blm_comm_ids.append(comm_id)

print(f"counter communities: {counter_comm_ids}")
print(f"{len(unknown_comm_ids)} unknown communities")
print(f"{len(blm_comm_ids)} BLM communities")

# +
movement_template = Template('''
## MOVEMENT $movement

Communities: $num_communities  
Members: $num_members  
Retweets: $num_retweets  
Tweets: $num_tweets

### Top Hashtags
| Count | Hashtag |
|------:|:------|
$hashtag_list

### Top Retweets
| Count | Tweet |
|------:|:------|
$retweet_list

''')

def store_movement_reports(movement, report_dir, comm_ids, tm):
    '''Write files with salient data on BLM or counter movement during a period
    
    Parameters:
    -----------
    movement : str
        "BLM" or "Counter"
    report_dir : str
        FS directory where reports are to be written
    comm_ids : list of int
        IDs of communities in movement
    tm : TweetManager instance
    '''
    # (up to) 300 most influential accounts (by number of retweets)
    num_retweeters = 300
    list_size = 0
    movement_ranking = []
    user_ranking = sorted(tm.user_retweeted_frequency.items(), key = lambda x:x[1], reverse = True)
    for user_id, num_tweets in user_ranking:
        if tm.user_community_map[user_id] in comm_ids:
            movement_ranking.append((user_id, num_tweets))
            list_size += 1
        if list_size == num_retweeters:
            break
    report_name = f"{movement}-influencers.txt"
    report_path = path.join(report_dir, report_name)
    with open(report_path, 'w', encoding="utf-8") as f:
        for user_id, num_tweets in movement_ranking:
            f.write(f"{user_id},{num_tweets}\n")
    # movement stats
    ## counts
    num_communities = len(comm_ids)
    num_members = 0
    num_tweets = 0
    num_retweets = 0
    retweet_counter = Counter()
    for community_id in comm_ids:
        num_members += len(tm.community_user_map[community_id])
        num_tweets += tm.comm_tweet_counter[community_id]
        comm_rt_counter = tm.community_retweet_counter[community_id]
        for tweet_id in comm_rt_counter:
            num_retweets += comm_rt_counter[tweet_id]
            retweet_counter[tweet_id] += comm_rt_counter[tweet_id]
        
    ## 25 most important hashtags
    num_examples = 25
    meme_counter = Counter()
    for community_id in tm.community_meme_counter:
        if community_id in comm_ids:
            c_counter = tm.community_meme_counter[community_id]
            for meme in c_counter:
                meme_counter[meme] += c_counter[meme]
    top_memes = sorted(meme_counter.items(), key = lambda x: x[1], reverse = True)
    hashtag_list = ""
    for i, (ht, count) in enumerate(top_memes):
        if i == num_examples:
            break
        hashtag_list += f"| {count} | {ht} |\n"
    
    ## 25 most retweeted
    top_retweets = sorted(retweet_counter.items(), key = lambda x: x[1], reverse = True)
    retweet_list = ""
    line_feeds = re.compile("[\r\n]")
    for i, (tweet_id, count) in enumerate(top_retweets):
        if i == num_examples:
            break
        tweet = get_tweet_text_by_id(tweet_id)
        tweet = line_feeds.sub('', tweet)
        retweet_list += f"| {count} | {tweet} |\n"
    
    ## Write to file
    subs = {
        "movement": movement,
        "num_communities": num_communities,
        "num_members": num_members,
        "num_tweets": num_tweets,
        "num_retweets": num_retweets,
        "hashtag_list": hashtag_list,
        "retweet_list": retweet_list,
    }    
    movement_summary = movement_template.safe_substitute(subs)
    report_name = f"{movement}_summary.md"
    report_path = path.join(report_dir, report_name)
    with open(report_path, 'w', encoding="utf-8") as f:
        f.write(movement_summary)
        
    # community ID -> member list
    members = {"community_id": [], "user_id":[], "tweet_count":[]}
    for cid in comm_ids:
        for user_id in tm.community_user_map[cid]:
            num_tweets = tm.user_tweet_counter[user_id]
            members["community_id"].append(cid)
            members["user_id"].append(user_id)
            members["tweet_count"].append(num_tweets)
    members_df = pd.DataFrame(members)
    members_file = f"{movement}-members.csv"
    members_file_path = path.join(report_dir, members_file)
    members_df.to_csv(members_file_path, index = False)
    return members_df
        

counter_member_df = store_movement_reports("Counter", report_dir, counter_comm_ids, tm)
blm_member_df = store_movement_reports("BLM", report_dir, blm_comm_ids, tm)


# -

# serialize the graph
graph_file_name = "graph.pkl"
graph_file_path = path.join(report_dir, graph_file_name)
tm.urg.g.write_pickle(graph_file_path, version = -1)

# +
# overview report
overview_template = Template('''
## OVERVIEW of PERIOD $start_date to $end_date

| What  | How Many |
|:-------|--------:|
| Tweets | $num_tweets |
| Retweets | $num_retweets |  
| Communities | $num_communities |  
| Accounts | $num_accounts |
| Size of largest community | $largest_comm_size |

''')
num_tweets = sum(tm.user_tweet_counter.values())
num_retweets = sum(tm.user_retweeted_frequency.values())
num_communities = len(tm.community_user_map)
num_accounts = len(tm.user_tweet_counter)
largest_comm_size = len(tm.community_user_map[0])

subs = {
    'start_date': start_date,
    'end_date': end_date,
    'num_tweets': num_tweets,
    'num_retweets': num_retweets,
    'num_communities': num_communities,
    'num_accounts': num_accounts,
    'largest_comm_size': largest_comm_size,
}
overview_report_name = "OverviewReport.md"
overview_path = path.join(report_dir, overview_report_name)
overview = overview_template.safe_substitute(subs)
with open(overview_path, 'w', encoding="utf-8") as f:
    f.write(overview)

# -

# inter-community dialog report
blm_member_df.columns

size_df = blm_member_df[["community_id", "user_id"]].rename(columns={'user_id':'size'}).groupby('community_id').count()
tweet_count_df = blm_member_df[["community_id", "tweet_count"]].groupby('community_id').sum()
blm_comm_df = size_df.merge(tweet_count_df, on="community_id")
blm_comm_df["avg_tweets"] = blm_comm_df["tweet_count"]/blm_comm_df["size"]
blm_comm_df["internal_retweets"] = list(map(lambda x: sum(tm.community_retweet_counter[x].values()), blm_comm_ids))
blm_comm_df["retweet_pct"] = blm_comm_df["internal_retweets"]/blm_comm_df["tweet_count"]
blm_comm_df

# +
reply_to_cp_counter = Counter()
reply_from_cp_counter = Counter()
for comm_pair in tm.inter_comm_reply_counter:
    replying_comm = comm_pair[0]
    reply_to_comm = comm_pair[1]
    if reply_to_comm in counter_comm_ids and replying_comm in counter_comm_ids:
        continue
    if reply_to_comm in counter_comm_ids:
        reply_to_cp_counter[replying_comm] += tm.inter_comm_reply_counter[comm_pair]
    if replying_comm in counter_comm_ids:
        reply_from_cp_counter[reply_to_comm] += tm.inter_comm_reply_counter[comm_pair]
        
blm_comm_df["replies_to_cp"] = list(map(lambda x: reply_to_cp_counter[x], blm_comm_ids))
blm_comm_df["replies_from_cp"] = list(map(lambda x: reply_from_cp_counter[x], blm_comm_ids))
blm_comm_df
# -

comm_retweeted_counter = Counter()
for comm_pair in tm.inter_comm_retweet_counter:
    retweeted_comm = comm_pair[1]
    comm_retweeted_counter[retweeted_comm] += tm.inter_comm_retweet_counter[comm_pair]
blm_comm_df["retweeted_external"] = list(map(lambda x: comm_retweeted_counter[x], blm_comm_ids))
blm_comm_df    


def is_blm_retweeting_cp(x):
    return x[0] in blm_comm_ids and x[1] in counter_comm_ids
communities_retweeting_cp = list(filter(is_blm_retweeting_cp, tm.inter_comm_retweet_counter.keys()))
cp_retweet_counts = Counter()
for pair in communities_retweeting_cp:
    retweet_comm = pair[0]
    cp_retweet_counts[retweet_comm] += tm.inter_comm_retweet_counter[pair]
blm_comm_df["cp_retweets"] = [cp_retweet_counts[c] for c in blm_comm_ids]
blm_comm_df["cp_retweet_pct"] = blm_comm_df["cp_retweets"] / (blm_comm_df["cp_retweets"] + blm_comm_df["internal_retweets"])
blm_comm_df

report_name = "BLM_Community_Summary.csv"
report_path = path.join(report_dir, report_name)
blm_comm_df.to_csv(report_path)



# +
# Configurable parameters
start_date = "2014-11-23"
end_date = "2014-12-23"
es_idx = 'tweets'
query_body = {
  "query": {
      "range": {
          "doc.created_at": {
            "gte": "Sun Nov 23 00:00:00 +0000 2014",
            "lt": "Wed Dec 24 00:00:00 +0000 2014"
          }
      }
  }
}
period = 2
num_init_communities = 25
num_exemplars = 10



# +
# Configurable parameters
start_date = "2012-08-20"
end_date = "2014-11-22"
es_idx = 'tweets'
query_body = {
  "query": {
      "range": {
          "doc.created_at": {
            "lt": "Sun Nov 23 00:00:00 +0000 2014"
          }
      }
  }
}
period = 1
num_init_communities = 25
num_exemplars = 10


