from string import Template
from typing import List

init_report_template = Template('''
## COMMUNITY $community_id

Members: $num_members  
Tweets: $num_tweets  
Retweets: $num_retweets  
Retweeted by other communities: $num_retweets_by_others  
Replies to other communities: $num_replies  
Replies from other communities: $num_replied_to  
Top 10 influence ranks: $top10_influence_ranks  

### Top Hashtags (by percentage inclusion in retweets)
$hashtag0 : $hashtag0_percent  
$hashtag1 : $hashtag1_percent  
$hashtag2 : $hashtag2_percent  
$hashtag3 : $hashtag3_percent  
$hashtag4 : $hashtag4_percent  
$hashtag5 : $hashtag5_percent  
$hashtag6 : $hashtag6_percent  
$hashtag7 : $hashtag7_percent  
$hashtag8 : $hashtag8_percent  
$hashtag9 : $hashtag9_percent  

### Top Retweets (by percentage inclusion in retweets)
$rt0_percent : $rt0  
$rt1_percent : $rt1  
$rt2_percent : $rt2  
$rt3_percent : $rt3  
$rt4_percent : $rt4  
$rt5_percent : $rt5  
$rt6_percent : $rt6  
$rt7_percent : $rt7  
$rt8_percent : $rt8  
$rt9_percent : $rt9  

''')

def generate_init_community_report(
    community_id, 
    num_members,
    num_tweets,
    num_retweets,
    hashtags, 
    ht_counts, 
    rts, 
    rt_counts,
    num_retweets_by_others,
    num_replies,
    num_replied_to,
    top10_influence_ranks: List[int],
):
    num_samples = 10
    subs = {
        "community_id": community_id,
        "num_members": num_members,
        "num_tweets": num_tweets,
        "num_retweets": num_retweets,
        "num_retweets_by_others": num_retweets_by_others,
        "num_replies": num_replies,
        "num_replied_to": num_replied_to,
    }
    ranks = [str(r) for r in top10_influence_ranks]
    subs["top10_influence_ranks"] =  ", ".join(ranks)
    for i in range(num_samples):
        if i == len(hashtags) or i == len(rts):
            break
        subs[f"hashtag{i}"] = hashtags[i]
        hashtag_pct = 100.0 * ht_counts[i] / num_retweets
        subs[f"hashtag{i}_percent"] = f"{hashtag_pct:2.3f}%".rjust(7, '0')
        retweet_pct = 100.0 * rt_counts[i] / num_retweets
        subs[f"rt{i}_percent"] = f"{retweet_pct:2.3f}%".rjust(7, '0')
        subs[f"rt{i}"] = rts[i]

    return init_report_template.safe_substitute(subs)
