import regex
from typing import Tuple, NamedTuple

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
    scores = EmoScores(
        trust = nrc.affect_frequencies["trust"] / total_emo_frequencies,
        anticipation = nrc.affect_frequencies["anticipation"] / total_emo_frequencies,
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


def eval_tweet_sentiment(tweet: str) -> Tuple[PronounCounts, EmoScores, float]:
    pc = _get_pronoun_counts(tweet)
    emo = _get_emotion_scores(tweet)
    sentiment = _get_sentiment(tweet)
    return pc, emo, sentiment
