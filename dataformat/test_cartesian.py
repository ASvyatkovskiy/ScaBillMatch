#!/usr/bin/env python

import cPickle as pickle

good_pairs = pickle.load(open( "good_pairs.p", "rb" ) )
print type(good_pairs)
print len(good_pairs)
flat = set([item for sublist in good_pairs for item in sublist])
print len(flat)
for pp in flat:
    print pp

