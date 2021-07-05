import regex
from typing import List, NamedTuple

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nrclex import NRCLex
from textblob import TextBlob

nltk.download("vader_lexicon")
sia = SIA()

class EmoScores(NamedTuple):
    trust: float = 0.0
    anticipation: float = 0.0
    joy: float = 0.0
    surprise: float = 0.0
    anger: float = 0.0
    disgust: float = 0.0
    fear: float = 0.0
    sadness: float = 0.0


class PronounCounts():
    
    def __init__(self, first_singular = 0, first_plural = 0, second = 0, third = 0):
    
        self.first_singular = first_singular
        self.first_plural= first_plural
        self.second = second
        self.third = third

    def __add__(self, other):
        return PronounCounts(
            first_singular = self.first_singular + other.first_singular,
            first_plural = self.first_plural + other.first_plural,
            second = self.second + other.second,
            third = self.third + other.third
        )

    def __eq__(self, o: object) -> bool:
        return self.first_singular == o.first_singular and \
            self.first_plural == o.first_plural and \
            self.second == o.second and \
            self.third == o.third

    
    def __truediv__(self, o: object):
        quotient = PronounCounts(
            self.first_singular / o,
            self.first_plural / o,
            self.second / o,
            self.third / o,
        )
        return quotient

    
    def get_proportions(self):
        sum = self.first_singular + self.first_plural + self.second + self.third
        if sum == 0:
            return self
        else:
            return self / sum


class SentimentAnalysis(NamedTuple):
    pronoun_counts: PronounCounts
    emo_scores: EmoScores
    sentiment: float 


class Pronoun():
    first_singular = "1s"
    first_plural = "1p"
    second = "2"
    third = "3"


pronouns = {
    "i" : Pronoun.first_singular,
    "me" : Pronoun.first_singular,
    "mine" : Pronoun.first_singular,
    "we" : Pronoun.first_plural,
    "us" : Pronoun.first_plural,
    "our" : Pronoun.first_plural,
    "ours" : Pronoun.first_plural,
    "you" : Pronoun.second,
    "your" : Pronoun.second,
    "yours" : Pronoun.second,
    "he" : Pronoun.third,
    "she" : Pronoun.third,
    "it" : Pronoun.third,
    "they" : Pronoun.third,
    "him" : Pronoun.third,
    "them" : Pronoun.third,
    "his" : Pronoun.third,
    "her" : Pronoun.third,
    "hers" : Pronoun.third,
    "its" : Pronoun.third,
    "their" : Pronoun.third,
    "theirs" : Pronoun.third,
}

def _get_pronoun_counts(tweet: str) -> PronounCounts:
    first_singular, first_plural, second, third = 0, 0, 0, 0
    tweet = tweet.lower().replace("let's", "let us").replace("y'all", "you all")
    for word in TextBlob(tweet).words:
        which = pronouns.get(word)
        if which is None:
            continue
        if which == Pronoun.first_singular:
            first_singular += 1
        elif which == Pronoun.first_plural:
            first_plural += 1
        elif which == Pronoun.second:
            second += 1
        elif which == Pronoun.third:
            third += 1
    return PronounCounts(first_singular, first_plural, second, third)


def _get_emotion_scores(tweet: str) -> EmoScores:
    nrc = NRCLex(tweet)
    total_emo_frequencies = sum(
        val for k, val in nrc.affect_frequencies.items() 
        if k not in ("positive", "negative")
    )
    if total_emo_frequencies == 0.0:
        return EmoScores()
    # workaround for apparent bug in NRCLex
    anticipation_key = "anticipation" if "anticipation" in nrc.affect_frequencies else "anticip"
    scores = EmoScores(
        trust = nrc.affect_frequencies["trust"] / total_emo_frequencies,
        anticipation = nrc.affect_frequencies[anticipation_key] / total_emo_frequencies,
        joy = nrc.affect_frequencies["joy"] / total_emo_frequencies,
        surprise = nrc.affect_frequencies["surprise"] / total_emo_frequencies,
        anger = nrc.affect_frequencies["anger"] / total_emo_frequencies,
        disgust = nrc.affect_frequencies["disgust"] / total_emo_frequencies,
        fear = nrc.affect_frequencies["fear"] / total_emo_frequencies,
        sadness = nrc.affect_frequencies["sadness"] / total_emo_frequencies
    )
    return scores


def _get_sentiment(tweet: str) -> float:
    return sia.polarity_scores(tweet)["compound"]


def eval_tweet_sentiment(tweet: str) -> SentimentAnalysis:
    pc = _get_pronoun_counts(tweet)
    emo = _get_emotion_scores(tweet)
    sentiment = _get_sentiment(tweet)
    return SentimentAnalysis(pc, emo, sentiment)


def summarize_sentiment(sentiment_analyses: List[SentimentAnalysis]) -> SentimentAnalysis:
    num_tweets = len(sentiment_analyses)
    pc = PronounCounts()
    trust: float = 0.0
    anticipation: float = 0.0
    joy: float = 0.0
    surprise: float = 0.0
    anger: float = 0.0
    disgust: float = 0.0
    fear: float = 0.0
    sadness: float = 0.0
    sentiment: float = 0.0
    for sa in sentiment_analyses:
        pc += sa.pronoun_counts
        trust += sa.emo_scores.trust
        anticipation += sa.emo_scores.anticipation
        joy += sa.emo_scores.joy
        surprise += sa.emo_scores.surprise
        anger += sa.emo_scores.anger
        disgust += sa.emo_scores.disgust
        fear += sa.emo_scores.fear
        sadness += sa.emo_scores.sadness
        sentiment += sa.sentiment
    pc = pc.get_proportions()
    trust /= num_tweets
    anticipation /= num_tweets
    joy /= num_tweets
    surprise /= num_tweets
    anger /= num_tweets
    disgust /= num_tweets
    fear /= num_tweets
    sadness /= num_tweets
    sentiment /= num_tweets
    emo_scores = EmoScores(trust, anticipation, joy, surprise, anger, disgust, fear, sadness)
    return SentimentAnalysis(pc, emo_scores, sentiment)
