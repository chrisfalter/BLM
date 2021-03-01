from datetime import date
import json
import pandas as pd

data_dir = "./Data/2014-2015/"
hist_json = data_dir + "tweets_histogram.json"
with open(hist_json, 'r') as f:
    d_hist = json.load(f)

buckets = d_hist["aggregations"]["tweets_per_day"]["buckets"]
dates = [date.fromtimestamp(bucket["key"]/1000) for bucket in buckets]
counts = [bucket["doc_count"] for bucket in buckets]
hist = pd.DataFrame({"date": dates, "count": counts})
hist.to_csv(data_dir + "tweets_counts.csv", index=False)

