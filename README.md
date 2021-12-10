# Black Lives Matter Tweet Analytics

## Unit Tests

Use the following command to run unit tests from the project root directory:
```
pytest ./tests
```

In some python environments, you may need to invoke the pytest module differently:
```
python -m pytest ./tests
```

## Loading Twitter Data

Tweets employing the hashtag #BlackLivesMatter or #BLM were loaded into Elasticsearch indices. Tweets from the period August 9, 2014 to May 8, 2015 were loaded into an index named `tweets`, and tweets from the period January 1, 2020 to October 31, 2020 were loaded into an index named `tweets2`. In compliance with the Twitter Developer Agreement and Policy, these indices cannot be shared. However, my corpus can be approximately re-created by downloading the tweets with the IDs as indicated below:

* Years 2014 - 2015 - The Python script `/src/archived_tweets.py` was used to download and process tweets having the IDs published by Alexandra Olteanu [on CrisisLex](https://crisislex.org/data-collections.html#BlackLivesMatter). In order to recover from inevitable network interruptions, a method to skip over tweets having IDs less than a specified ID was used:
```
if tweet_ids[-1] <= 597189127243575296: #last successful ID before connection error
    continue
```
The value of the tweet ID in the script was updated each time an interruption occurred.

* Year 2020 - The Observatory on Social Media (OSOME) at Indiana University maintains a store of tweets from the Twitter Decahose API. OSOME was searched for tweets having one or both of the hashtags `#BlackLivesMatter` and `#BLM` during the relevant period. Zip files containing search results were downloaded to disk, then the results were loaded to the `tweets2` Elasticsearch index using the `data/process_osome_gz_dir.py` script. The IDs of the tweets are available in the file `data/tweets2_tweet_ids_part1.txt` and `data/tweets2_tweet_ids_part2.txt`.


## Analysis of Twitter Data

A primary goal of this research is to detect trends and behaviors in Twitter use during 3 chronological phases:
1. *Struggle* - Activists are less numerous and largely absent from the public stage.
2. *Surge* - A prominent event (the Michael Brown shooting in Ferguson; the George Floyd murder in Minneapolis) drives intense interest and activity for a few weeks.
3. *Consolidation* - Interest and activity wain somewhat, but at a level significantly higher than the surge level.

Change point analysis was applied to the tweet count in each of the 2 periods to divide them into 3 phases. The `src/tweet_counts.ipynb` notebook contains the relevant code.

Once the boundaries of the phases were determined, tweets were analyzed for each of the 6 phases. A network of account nodes and retweet edges was constructed, and communities identified. Natural language processing algorithms for sentiment analysis, emotion recognition, and pronoun usage were applied to individual accounts and aggregated at the community level. 

The notebooks `src/BLM.ipynb`, `src/BLM_2.ipynb`, etc. contain the relevant code. Each notebook was executed in several steps:
* Phase parameters (Elasticsearch query, period ID, etc.) were edited appropriately.
* Matching tweets were processed in an instance of the `TweetsManager` class.
* Community size was analyzed, and the smallest (least relevant) communities were discarded with minimal loss of tweets.
* An initial report (`Largest Communities Hashtags and Tweets.md`) was generated for the 40 largest communities.
* A qualitative assessment of stance (activist, counter-protest, or unknown/excluded) was made for the 40 communities and noted in Python lists.
* A Naive Bayes model (in `src/blm_classifier.py`) was generated and used to predict stance on the remaining communities.
* Further analysis of Twitter activity, sentiment, emotions, and pronoun usage was performed and recorded in markdown files in the appropriate `/data/Reports/` sub-directory.

After this data was generated, I performed further analysis and visualizations in the following Python notebooks:
* `src/community_filter.ipynb` - Visualization of distribution of communities by activity in each phase
* `src/overview_reports.ipynb` - What it sounds like!
* `src/tweet_counts.ipynb` - Visualization of activist and counter-protest hashtags 2014 - 2020
* `src/pronouns.ipynb` - Social awareness analysis, using pronoun frequency as a proxy
* `src/sentiment_analysis.ipynb` - Sentiment polarity analysis
* `src/viz.ipynb` - Visualizations of activity and of emotion recognition

## Pre-publication draft

The draft is available at `BLM_Longitudinal_Analysis.pdf`.