import pytest

from src.tweet_sentiment import (
    PronounCounts,
    _get_emotion_scores,
    _get_pronoun_counts,
    _get_sentiment
)


sample_text = "It was the best of times, it was the worst of times, " \
    "it was the age of wisdom, it was the age of foolishness, " \
    "it was the epoch of belief, it was the epoch of incredulity, " \
    "it was the season of Life, it was the season of Darkness, " \
    "it was the spring of hope, it was the winter of despair, " \
    "we had everything before us, we had nothing before us, " \
    "we were all going direct to Heaven, we were all going direct the other way --" \
    "in short, the period was so far the like present period, that some of its noisiest authorities " \
    "insisted on its being received, for good or for evil, in the superlative degree of comparison only. " \
    "I'd always say, you'll only live once. Let's go, y'all!" # Added for testing; Dickens did not say this!

def test_getEmotionScores_shouldReturnPositiveValues_whenAllEmotionsIncluded():
    scores = _get_emotion_scores(sample_text)
    assert scores.trust > 0.0
    assert scores.anticipation > 0.0
    assert scores.joy > 0.0
    assert scores.surprise > 0.0
    assert scores.anger > 0.0
    assert scores.disgust > 0.0
    assert scores.fear > 0.0
    assert scores.sadness > 0.0


def test_getPronounCounts_shouldBeAccurate_whenContractionsUsed():
    counts = _get_pronoun_counts(sample_text)
    assert counts.first_singular == 1
    assert counts.first_plural == 7
    assert counts.second == 2
    assert counts.third == 12


def test_pronounCounts_shouldDoMathOperations():
    pc1 = PronounCounts(1, 2, 3, 4)
    pc2 = PronounCounts(1, 2, 3, 4)
    assert pc1 == pc2
    the_sum = pc1 + pc2 
    assert the_sum == PronounCounts(2, 4, 6, 8)
    assert the_sum / 2 == pc1
    pc1 += pc2
    assert pc1 == PronounCounts(2, 4, 6, 8)


def test_getSentiment_shouldReturnPositive_whenTweetIsHappy():
    tweet = "I'm so happy today!"
    sentiment = _get_sentiment(tweet)
    assert sentiment > 0.0
    assert sentiment <= 1.0


def test_getSentiment_shouldReturnNegative_whenTweetIsSad():
    tweet = "I'm sad, tears are pouring from my eyes, I'm so sad."
    sentiment = _get_sentiment(tweet)
    assert sentiment < 0.0
    assert sentiment >= -1.0
    