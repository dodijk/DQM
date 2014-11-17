
# coding: utf-8

# In[7]:

import codecs

from nltk import wordpunct_tokenize

# Based on https://github.com/semanticize/semanticizer/blob/master/semanticizer/wpm/load.py#L128
"""
line = 'Activiteitscoefficient,0,0,0,0,v{s{248591,0,0,F,T}}
{text},{LinkOccCount},{LinkDocCount},{TextOccCount},{TextDocCount}[{sense}, {sense}, ...]
---- 
sense = s{248591,0,0,F,T}
[{pageid}, {sLinkOccCount}, {sLinkDocCount}, {FromTitle}, {FromRedirect}]
"""
print '\nLoading labels ...'

counts = {}
file = codecs.open("nlwiki-latest/label.csv", "r", "utf-8")
for linenr, line in enumerate(file):
    try:
        stats_part, senses_part = line.split(',v{')
        senses = senses_part[:-1].split('s')[1:]
        stats = stats_part[1:].split(',')
        text = stats[0]
        link_tf, link_df, text_tf, text_df = map(int, stats[1:5])
        for term in wordpunct_tokenize(text):
            term = term.lower().strip()
            if term not in counts:
                counts[term] = [link_tf, link_df, text_tf, text_df]
            else:
                counts[term][0] += link_tf
                counts[term][1] += link_df
                # Note that text_tf & text_df are an underestimate, as only terms in
                # links are included in label.csv.
                counts[term][2] += text_tf
                counts[term][3] += text_df
    except Exception,e:
        print "Error loading on line " + str(linenr+1) + ": " + line
        print str(e)
        raise
print '\nDone loading labels (%d labels loaded)' % (linenr+1)


# In[8]:

article_count = int(open("nlwiki-latest/stats.csv").readline()[14:-1])
sum_link_tf, sum_link_df, sum_text_tf, sum_text_df = map(sum, zip(*counts.values()))


# In[ ]:

import json, urllib2
from flask import Flask

from math import log, e, exp

app = Flask("Flask")

@app.route("/")
def wikipedia_count_usage():
    return("Usage: /<query> for count.")
    
def feature_transform((feature, value)):
    new_features = []
    new_features += ("log_"+feature, log(value) if value > 0 else 0.0)
    return new_features

@app.route("/<query>")
def wikipedia_count(query):
    cnts = {}
    cnts["anchor_tf"], cnts["anchor_df"], cnts["text_tf"], cnts["text_df"] = counts.get(query, [0, 0, 1, 1])
    cnts["anchor_idf"] = log(float(article_count)/cnts["anchor_df"]) if cnts["anchor_df"] else 0.0
    if cnts["anchor_tf"] == 0: cnts["anchor_ridf"] = 0.0
    else: cnts["anchor_ridf"] = cnts["anchor_idf"] - log(1/(exp(float(cnts["anchor_tf"])/article_count)-1))
    cnts["text_idf"] = log(float(article_count)/cnts["text_df"]) if cnts["text_df"] else 0.0
    if cnts["text_tf"] == 0: cnts["text_ridf"] = 0.0
    else: cnts["text_ridf"] = cnts["text_idf"] - log(1/(exp(float(cnts["text_tf"])/article_count)-1))
    cnts.update(map(feature_transform, cnts.iteritems()))
    #return json.dumps(cnts)    
    return cnts
    
def feature_ranges():
    ranges = {}
    for term in counts.keys():
        for f, v in wikipedia_count(term).iteritems():
            if f not in ranges:
                ranges[f] = v, v
            else:
                ranges[f] = min(ranges[f][0], v),  max(ranges[f][1], v)
    return ranges

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

