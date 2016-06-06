#!/usr/bin/env python

import simplejson

#bills/old/IL/2003/HB5102/HB5102_Introduced.txt^^^

'''
{"namespace": "bills.avro",
 "type": "record",
 "name": "Bill",
 "fields": [
     {"name": "year", "type": "int"},
     {"name": "state",  "type": "string"},
     {"name": "docid", "type": "string"},
     {"name": "docversion", "type": "string"},
     {"name": "content", "type": "string"},
     {"name": "primary_key", "type": "string"}
 ]
}
'''
us_state_abbrev = {
    'AL':0,
    'AK':1,
    'AZ':2,
    'AR':3,
    'CA':4,
    'CO':5,
    'CT':6,
    'DE':7,
    'FL':8,
    'GA':9,
    'HI':10,
    'ID':11,
    'IL':12,
    'IN':13,
    'IA':14,
    'KS':15,
    'KY':16,
    'LA':17,
    'ME':18,
    'MD':19,
    'MA':20,
    'MI':21,
    'MN':22,
    'MS':23,
    'MO':24,
    'MT':25,
    'NE':26,
    'NV':27,
    'NH':28,
    'NJ':29,
    'NM':30,
    'NY':31,
    'NC':32,
    'ND':33,
    'OH':34,
    'OK':35,
    'OR':36,
    'PA':37,
    'RI':38,
    'SC':39,
    'SD':40,
    'TN':41,
    'TX':42,
    'UT':43,
    'VT':44,
    'VA':45,
    'WA':46,
    'WV':47,
    'WI':48,
    'WY':49
}

#switch between primary key as a long integer and primary key as a string
use_cryptic_pk = True

import glob
inputs = glob.glob("/scratch/network/alexeys/bills/lexs/text_3states_partitioned/*")

foutput_meta = open("/scratch/network/alexeys/bills/lexs/bills_metadata.json","wa")

#metadata
output_dicts_meta = list()
for input in inputs:
    with open(input,mode="r") as finput:
        lines = finput.readlines()
        for line in lines:
            sid, content = line.split("^^^")
            try: 
                bla, state, year, docid, docversion_pre = sid.split("/")
            except:
                blah, bla, state, year, docid, docversion_pre = sid.split("/")
            #if state == 'IL': continue
            output_dict = {'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':None}
            output_dict['year'] = int(year)
            output_dict['state'] = us_state_abbrev[state]
            output_dict['docid'] = docid
            version = docversion_pre.split("_")[1].rstrip(".txt")
            version = "".join(version.split())
            output_dict['docversion'] = version
            output_dict['primary_key'] = state+"_"+year+"_"+docid+"_"+version
            output_dicts_meta.append(output_dict)

for i, output_dict in enumerate(output_dicts_meta):
    if use_cryptic_pk: output_dict['primary_key'] = i
    simplejson.dump(output_dict, foutput_meta)
    foutput_meta.write('\n')

foutput = open("/scratch/network/alexeys/bills/lexs/bills.json","wa")

output_dicts = list()
for input in inputs:
    with open(input,mode="r") as finput:
        lines = finput.readlines()
        for line in lines:
            sid, content = line.split("^^^")
            try:
                bla, state, year, docid, docversion_pre = sid.split("/")
            except: 
                blah, bla, state, year, docid, docversion_pre = sid.split("/")
            #if state == 'IL': continue
            output_dict = {'primary_key':'', 'content':None}
            version = docversion_pre.split("_")[1].rstrip(".txt")
            output_dict['content'] = content.decode("utf-8",errors='replace')
            output_dict['primary_key'] = state+"_"+year+"_"+docid+"_"+version
            output_dicts.append(output_dict)

for i, output_dict in enumerate(output_dicts):
    if use_cryptic_pk: output_dict['primary_key'] = i
    simplejson.dump(output_dict, foutput)
    foutput.write('\n')
