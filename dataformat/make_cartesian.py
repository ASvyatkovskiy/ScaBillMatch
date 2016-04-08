#!/usr/bin/env python

import simplejson
import cPickle as pickle

data_dict = list()
with open("/scratch/network/alexeys/bills/lexs/text_3states_partitioned/bills_metadata.json","r") as fp:
#with open("bills_metadata.json","r") as fp:
    jsons = fp.readlines()
    for js in jsons:
        #print js
        #output_dict = {'year':None, 'state':'', 'docversion':'', 'primary_key':None}
        data_as_dict = simplejson.loads(js)
        #print data_as_dict
        data_dict.append(data_as_dict)

good_pairs = list()
def do_event(i,ievent,good_pairs):
    #print "Processing ", i, " out of ", len(data_dict) 
    idocversion = ievent['docversion']
    if 'Enacted' in idocversion:
        istate = ievent['state']
        iyear = ievent['year']
        for j,jevent in enumerate(data_dict):
            #if i > j: continue
            jdocversion = jevent['docversion']
            if 'Enacted' not in jdocversion: continue
            jstate = jevent['state']
            jyear = jevent['year']
            if istate != jstate and iyear < jyear:
                #print "ievent['primary_key'] ", ievent['primary_key']
                #print "jevent['primary_key'] ", jevent['primary_key'] 
                output_dict = {'pk1':'', 'pk2':''}
                output_dict['pk1'] = ievent['primary_key']
                output_dict['pk2'] = jevent['primary_key']
                good_pairs.append(output_dict)
    return good_pairs

from joblib import Parallel, delayed  
import multiprocessing
num_cores = 20 #multiprocessing.cpu_count()
part = len(data_dict)/2
good_pairs = Parallel(n_jobs=num_cores)(delayed(do_event)(i,ievent,good_pairs) for i,ievent in enumerate(data_dict[:part]))

foutput1 = open("good_pairs_part1.json","wba")
#pickle.dump(good_pairs, open("good_pairs_1.p", "wb"))
for output_dict in good_pairs:
    simplejson.dump(output_dict, foutput1)
    foutput1.write('\n')

good_pairs = list()
good_pairs = Parallel(n_jobs=num_cores)(delayed(do_event)(i,ievent,good_pairs) for i,ievent in enumerate(data_dict[part:]))
foutput2 = open("good_pairs_part2.json","wba")
#pickle.dump(good_pairs, open("good_pairs_2.p", "wb"))
for output_dict in good_pairs:
    simplejson.dump(output_dict, foutput2)
    foutput2.write('\n')

#flat = set([item for sublist in good_pairs for item in sublist])
#for pp in flat:
#    print pp
