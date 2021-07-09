import pytest

from src.tweet_sentiment import (
    EmoScores,
    PronounCounts,
    SentimentAnalysis,
    summarize_sentiment,
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


def test_getEmotionScores_shouldReturnSomeZeros_whenSomeEmotionsAbsent():
    sample = "I love my family. They make me so happy!"
    scores = _get_emotion_scores(sample)
    assert scores.sadness == 0.0
    assert scores.fear == 0.0
    assert scores.anger == 0.0
    assert scores.disgust == 0.0
    assert scores.joy > 0.0
    assert scores.trust > 0.0


def test_getEmotionScores_returnsZeros_whenTextIsVeryShort():
    sample = "Gondwana"
    scores = _get_emotion_scores(sample)
    assert scores.trust == 0.0
    assert scores.anticipation == 0.0
    assert scores.surprise == 0.0
    assert scores.sadness == 0.0
    assert scores.fear == 0.0
    assert scores.anger == 0.0
    assert scores.disgust == 0.0
    assert scores.joy == 0.0


def test_getEmotionScores_handlesAnticipationCorrectly():
    # the affect_frequency dict sometimes uses a key of 'anticip' rather than 'anticipation'
    # this behavior seems like a bug in NRCLex.
    text = 'Arrested for hanging a banner. While Darren Wilson still remains free ' \
        'for killing a Black teen. #BlackLivesMatter http://t.co/de8xpE2v5r'
    scores = _get_emotion_scores(text)
    assert scores.anticipation == 0.0


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
    assert pc2 * 2 == PronounCounts(2, 4, 6, 8)


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


def test_pronounCounts_shouldReturnNormalizedValues_whenGetProportionsCalled():
    pc = PronounCounts(1, 2, 3, 4)
    result = pc.get_proportions()
    assert result == PronounCounts(0.1, 0.2, 0.3, 0.4)


def test_pronounCountsGetProportions_shouldReturnZeros_whenNoPronouns():
    pc = PronounCounts(0, 0, 0, 0)
    result = pc.get_proportions()
    assert result == pc


def test_emoScores_shouldPerformMathOps_whenUsedWithMathSyntax():
    emos = EmoScores(0.2, 0.2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)
    twice_emos = emos * 2
    assert twice_emos == EmoScores(0.2 * 2, 0.2 * 2, 0.1 * 2, 0.1 * 2, 0.1 * 2, 0.1 * 2, 0.1 * 2, 0.1 * 2)
    assert twice_emos / 2 == emos
    assert emos + emos == twice_emos


def test_summarizeSentiment_shouldNormalizeSummedSentiments_whenGivenList():
    sa1 = SentimentAnalysis(
        pronoun_counts=PronounCounts(0, 1, 2, 5),
        emo_scores=EmoScores(0, 0.25, 0, 0.25, 0, 0.25, 0, 0.25),
        sentiment=0.4
    )
    sa2 = SentimentAnalysis(
        pronoun_counts=PronounCounts(0, 1, 2, 5),
        emo_scores=EmoScores(0.25, 0, 0.25, 0, 0.25, 0, 0.25, 0),
        sentiment=0.2
    )
    expected = SentimentAnalysis(
        pronoun_counts=PronounCounts(0.0, 0.125, 0.25, 0.625),
        emo_scores=EmoScores(0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125),
        sentiment=0.3
    )
    actual = summarize_sentiment([sa1, sa2])
    assert actual.pronoun_counts == expected.pronoun_counts
    assert actual.emo_scores == expected.emo_scores
    assert actual.sentiment == pytest.approx(expected.sentiment)
