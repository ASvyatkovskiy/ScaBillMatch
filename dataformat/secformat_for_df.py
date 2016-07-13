#!/usr/bin/env python

import simplejson
import re

#switch between primary key as a long integer and primary key as a string
use_cryptic_pk = True

finput = open("/scratch/network/alexeys/bills/lexs/bills_50000.json","r")

#data first
foutput = open("/scratch/network/alexeys/bills/lexs/sectioned_bills_50000.json","wa")

#step through JSON. Tokenize "content" string
sec_output_dicts = list()
for line in finput.readlines():
    output_dict = simplejson.loads(line)
    for j,section in enumerate(re.split("SECTION \d|section \d",output_dict['content'])):
        sec_output_dict = {'primary_key':'', 'content':None} 

        sec_output_dict['content'] = section
        sec_output_dict['primary_key'] = str(j)+"_"+output_dict['primary_key']
        sec_output_dicts.append(sec_output_dict)

for i, sec_output_dict in enumerate(sec_output_dicts):
    if not use_cryptic_pk: sec_output_dict['primary_key'] = i
    simplejson.dump(sec_output_dict, foutput)
    foutput.write('\n')

#metadata
#FIXME yes...
finput = open("/scratch/network/alexeys/bills/lexs/bills_50000.json","r")
finput_meta = open("/scratch/network/alexeys/bills/lexs/bills_metadata_50000.json","r")
foutput_meta = open("/scratch/network/alexeys/bills/lexs/sectioned_bills_metadata_50000.json","wa")

#step through JSON. Tokenize "content" string
sec_output_dicts_meta = list()
for line_meta, line in zip(finput_meta.readlines(),finput.readlines()):
    output_dict_meta = simplejson.loads(line_meta)
    output_dict = simplejson.loads(line)
    for j in range(len(re.split("SECTION \d|section \d",output_dict['content']))):
        sec_output_dict_meta = {'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':''}
        sec_output_dict_meta['primary_key'] = str(j)+"_"+output_dict_meta['primary_key']
        sec_output_dict_meta['year'] = output_dict_meta['year']
        sec_output_dict_meta['state'] = output_dict_meta['state']
        sec_output_dict_meta['docid'] = output_dict_meta['docid']
        sec_output_dict_meta['docversion'] = output_dict_meta['docversion']
        sec_output_dicts_meta.append(sec_output_dict_meta)

for i, sec_output_dict_meta in enumerate(sec_output_dicts_meta):
    if not use_cryptic_pk: sec_output_dict_meta['primary_key'] = i
    simplejson.dump(sec_output_dict_meta, foutput_meta)
    foutput_meta.write('\n')
