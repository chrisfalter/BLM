from collections import defaultdict
from nltk import download, pos_tag
from nltk.tokenize import wordpunct_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords, wordnet
import string
import re
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
import random

download('wordnet')
download('averaged_perceptron_tagger')
download("stopwords")

class TransformerBase(BaseEstimator, TransformerMixin):
    '''
    Provides no-op fit() function for Transformers that only need
    a fit method
    '''
    
    def __init__(self):
        pass
    
    def fit(self, X, y=None, **fit_params):
        return self


class LowerCaser(TransformerBase):
    
    def transform(self, X, **fit_params):
        for i in range(len(X)):
            X[i] = X[i].lower()
        return X    


class Tokenizer(TransformerBase):

    def transform(self, X, **fit_params):
        for i in range(len(X)):
            X[i] = wordpunct_tokenize(X[i])
            for tok in X[i]:
                if tok.endswith('.') and len(tok) > 1:
                    X[i].remove(tok)
        return X


def remove_listed_chars(X, removal_list):
    '''
    Parameters
      X - list of lists
      removal_list - string of characters (e.g., punctuation)
    '''
    remove_regex = re.compile('[' + removal_list + ']')
    for i, doc in enumerate(X):
        new_doc = []
        for tok in doc:
            new_tok = remove_regex.sub('', tok)
            if new_tok:
               new_doc.append(new_tok)
        X[i] = new_doc
    return X


class Stringizer(TransformerBase):
    def transform(self, X, **fit_params):
        for i in range(len(X)):
            X[i] = ' '.join(X[i])
        return X


class Lemmatizer(TransformerBase):
    
    def __init__(self):
        self.treenet_map = defaultdict(str)
        self.treenet_map['N'] = wordnet.NOUN
        self.treenet_map['R'] = wordnet.ADV
        self.treenet_map['V'] = wordnet.VERB
        self.treenet_map['J'] = wordnet.ADJ
        
    def transform(self, X, **fit_params):
        lemmatizer = WordNetLemmatizer()
        for i in range(len(X)):
            doc = X[i].copy()
            X[i] = [] # a list of lemmatized tokens
            for tok, pos in pos_tag(doc):
                wordnet_pos = self.treenet_map[pos[0]]
                if not wordnet_pos:
                    X[i].append(tok) # use tok without any lemmatizing if not a recognized POS
                else:
                    X[i].append(lemmatizer.lemmatize(tok, wordnet_pos))
        return X


class PunctuationRemover(TransformerBase):

    def __init__(self, exceptions = ''):
        self.exceptions = exceptions
        
    def transform(self, X, **fit_params):
        if not self.exceptions:
            punc = string.punctuation
        else:
            retained_punc = re.compile('['+self.exceptions+']') 
            punc = retained_punc.sub('', string.punctuation)
        return remove_listed_chars(X, punc)
    
    def get_params(self, deep=True):
        return {'exceptions': self.exceptions}
    
    def set_params(self, **parameters):
        for parm, value in parameters.items():
            setattr(self, parm, value)
        return self


def get_blm_classifier(blm_tweets, counter_tweets):
    text_pipeline = Pipeline([('lower', LowerCaser()),
                            ('tokenize', Tokenizer()),
                            ('lemmatize', Lemmatizer()),
                            ('punc', PunctuationRemover()),
                            ('stringize', Stringizer()),
                            ('vec', TfidfVectorizer()),
                            ('model', MultinomialNB())])
    en_stops = stopwords.words("english")
    param_grid = {
        "model__alpha": [1.0],
        "model__fit_prior": [False], # always use a uniform prior
        "vec__stop_words": [
            ["rt", "#blacklivesmatter", "blacklivesmatter"], 
            ["rt", "blacklivesmatter", "#blacklivesmatter"] + en_stops, 
            en_stops,
        ]
    }
    gs = GridSearchCV(text_pipeline, param_grid, cv = 5)
    X = blm_tweets + counter_tweets
    Y = [True] * len(blm_tweets) + [False] * len(counter_tweets)
    gs.fit(X, Y)
    return gs, gs.cv_results_
