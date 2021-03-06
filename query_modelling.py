#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
three endpoints:
- localhost:5000/query_reformulation/
- localhost:5000/query_modelling/
- localhost:5000/query_modelling/<session>


All endpoints accept only POST requests, with incoming data being a
json encoded object. Similarly, all endpoints return their result as a
JSON encoded object, see the examples below.


`query_reformulation` accepts:
    {
      "query": "De aap kreeg een noot van Mies.",
    }

and returns:
    {
      "weighted_query": "noot^25.452746 Mies.^25.452746 aap^25.452746 \
krijgt^8.871926 De^5.768321 een^4.812184 van^4.406719"
    }


`query_modelling` accepts:
    {
      "session": [
        {
          "query": "De aap kreeg een noot van Mies",
          "datetime": "2014-02-07T11:08:55+0000"
        },
        {
          "query": "Wim bakt koekjes met de zus van Jet.",
          "datetime": "2014-02-08T10:11:04+0000"
        },
        ...,
        ]
     }

and returns:
    {
      "weighted_query": "noot^25.452746 aap^25.452746 Mies^25.452746 \
kreeg^10.041335 De^5.768321 een^4.812184 van^4.441074 koekjes^0.198432 \
Jet.^0.198432 bakt^0.198432 zus^0.089670 Wim^0.082103 met^0.042888 de^0.031920"
    }


`query_modelling/<session>` accepts a session ID in the url and plain
query as `query_reformulation`. It is an implicit alternative of the
plain `query_modelling` endpoint. Accepts:

    {
      "query": "De aap kreeg een noot van Mies.",
    }


NB: datetime should be a ISO 8601 formatted string with timezone designator

"""
from collections import defaultdict
from datetime import datetime
from itertools import imap
import logging
import math

from dateutil import parser
from flask import Flask, request, jsonify
import pytz
import ujson # faster than json, but functionally equivalent
import yaml

from nltk import wordpunct_tokenize

from wikipediaCount import wikipedia_count, feature_ranges

class Corpus(object):
    """Small helper class to hold corpus specific information (read:
    word counts).

    """
    def __init__(self):
        self.ranges = feature_ranges()
        
    def __getitem__(self, key):
        return self.idf(key)
                    
    def feature_normalize(self, (feature, value)):
        feature_range = self.ranges[feature][1] - self.ranges[feature][0]
        if feature_range:
            new_value = (value - self.ranges[feature][0])/feature_range
        else: new_value = 0.0
        return feature, new_value

    def score(self, term, weights):
        """Return the score for the `term`."""
        cnts = wikipedia_count(term.lower())
        cnts.update(map(self.feature_normalize, cnts.iteritems()))
        cnts["is_capitalized"] = term != term.lower()
        score = sum(v*weights[f] for f, v in cnts.iteritems() if f in weights)
        return term.lower(), score

class QueryModeller(object):
    """Provides two methods to separate the chaff from the wheat in search
    queries.

    reformulate(query): reweighs the terms of a single query. Returns
    output in a format usable by Lucene.

    model(queries): reweighs the terms of a set of queries
    representing a session, or history of queries. Returns a single
    weighted query in the same format as reformulate(query).

    """
    def __init__(self, corpus=None, weights={}, top_n=25, 
                 decay_base=0.81, decay_scale=(1.0 / 60)):
        self.corpus = self.set_corpus(corpus)
        self.weights = weights
        self.decay_base = decay_base
        self.decay_scale = decay_scale
        self.top_n = top_n
        self.skip_terms = (',', '.', '...', "'", '"', '-', '!', ':', 
                           '(', ')', '?', '*', '%', "':", '\\')


    def set_corpus(self, corpus):
        self.corpus = corpus

    def tokenize(self, string):
        #return string.split()
        if len(string) == 0: return []
        tokens = wordpunct_tokenize(string[0].lower() + string[1:])
        return [term for term in tokens if term not in self.skip_terms]

    def weighted_terms(self, query):
        return imap(self.weigh, self.tokenize(query))

    def terms_to_query(self, weighted_terms):
        if "field" not in self.weights:
            return " ".join("%s^%f" % (term, weight) for (term, weight) in
                            weighted_terms if weight > 0)
        else:
            query_terms = [["%s:%s^%f" % (field, term, weight*field_weight) for (term, weight) in
                            weighted_terms if (weight*field_weight > 0)]
                            for (field, field_weight) in self.weights["field"].iteritems()]
            return " ".join(" ".join(q) for q in query_terms)            

    def get_top_n(self, weighted_terms):
        return sorted(weighted_terms.items(),
                      key=lambda (t, w): w, reverse=True)[:self.top_n]

    def reformulate(self, query):
        """Reformulate query by adjusting the weights of the terms.
        """
        return self.terms_to_query(
            self.get_top_n({t: w for (t, w) in self.weighted_terms(query)}))

    def _decay(self, old, new):
        delta = (new - old).total_seconds()
        return math.pow(self.decay_base, self.decay_scale * delta)

    def model(self, queries):
        """(Re)Model a query by taking into account the history (session) of
        past queries.

        """
        query_terms = defaultdict(float)

        queries = [(obj['query'], parser.parse(obj['datetime']))
                   for obj in queries]
        ordered_queries = sorted(queries, key=lambda (q, dt): dt)
        most_recent_dt = ordered_queries[-1][1]

        for query, query_dt in ordered_queries:
            decay = self._decay(query_dt, most_recent_dt)

            for term, weight in self.weighted_terms(query):
                #query_terms[term] += weight * decay
                query_terms[term] = max(weight * decay, query_terms[term])

        return self.terms_to_query(self.get_top_n(query_terms))

    def weigh(self, term):
        """Adjust the weight of the supplied term based on the IDF of our
        corpus.

        """
        return self.corpus.score(term, self.weights)


app = Flask(__name__)
QM = QueryModeller()
sessions = defaultdict(list)

def now(tz=None):
    if tz is None:
        tz = pytz.utc
    return datetime.now(tz)


def _get_data():
    """Read request data from client.

    Works regardless of the Content-Type header that the client sent.

    """
    #return ujson.loads(request.stream.read())
    return {"query": request.stream.read()}

def load_config(fname):
    with open(fname) as f:
        config = yaml.load(f.read())
    return config


@app.route('/')
def about():
    return "try POST'ing to /query_modelling or /query_reformulation"


@app.route('/query_reformulation', methods=["POST"])
def query_reformulation():
    data = _get_data()
    query = data['query']

    return jsonify(weighted_query=QM.reformulate(query))


@app.route('/query_modelling', methods=["POST"])
def query_modelling():
    data = _get_data()
    query = data['session']

    return jsonify(weighted_query=QM.model(query))

@app.route('/query_modelling/<session>', methods=["POST"])
def query_modelling_with_session(session):
    data = _get_data()
    data["datetime"] = now().isoformat()
    sessions[session].append(data)

    return jsonify(weighted_query=QM.model(sessions[session]))


def main():
    logging.basicConfig(level=logging.DEBUG)

    config_fname = 'query_modelling.yaml'
    logging.info('loading configuration from "%s"', config_fname)
    config = load_config(config_fname)
    app.config.update(config['flask'])
    logging.debug("app config: %s", app.config)

    logging.info('loading corpus data')
    corpus = Corpus()
    QM.set_corpus(corpus)
    QM.weights = config["weights"]

    logging.info('starting server')
    app.run(app.config.get('APP_HOST'), \
            app.config.get('APP_PORT'), \
            use_reloader=False)


if __name__ == "__main__":
    main()
