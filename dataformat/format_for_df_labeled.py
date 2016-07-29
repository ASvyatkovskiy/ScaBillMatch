#!/usr/bin/env python

import simplejson
import re
import pandas as pd

import glob
import parquet
parquet_files = glob.glob("/user/alexeys/output_cosine_FL_MI_SC_KMeansLabels/*parquet")
parquet_input_dicts = list()

for f in parquet_files:
    with open(f) as pf:
        for row in parquet.DictReader(pf, columns=['primary_key', 'prediction']):
            parquet_input_dicts.append(dict(row)) 

parquet_DF = pd.DataFrame(parquet_input_dicts)
#print parquet_DF.count()

finput = open("/scratch/network/shiraito/text/data/jsonFiles/FL_MI_SC_bills.json","r")

input_bill_dicts = list()
for line in finput.readlines():
    input_dict = simplejson.loads(line)
    input_bill_dicts.append(input_dict)

input_bills_DF = pd.DataFrame(input_bill_dicts)
#print input_bills_DF.count()

labeled_bills_DF = parquet_DF.merge(input_bills_DF, on="primary_key", how="left")
#print labeled_bills_DF.count()

#data first
foutput = open("/scratch/network/alexeys/bills/lexs/labeled_bills_FL_MI_SC.json","wa")

print labeled_bills_DF.columns
for i in labeled_bills_DF.T.to_dict().values():
    simplejson.dump(i, foutput)
    foutput.write('\n')


#metadata
finput_meta = open("/scratch/network/shiraito/text/data/jsonFiles/FL_MI_SC_metadata.json","r")

input_meta_dicts = list()
for line_meta in finput_meta.readlines():
    input_dict_meta = simplejson.loads(line_meta)
    input_meta_dicts.append(input_dict_meta)

input_meta_DF = pd.DataFrame(input_meta_dicts)

labeled_meta_DF = parquet_DF.merge(input_meta_DF, on="primary_key", how="left")
print labeled_meta_DF.head()

foutput_meta = open("/scratch/network/alexeys/bills/lexs/labeled_bills_metadata_FL_MI_SC.json","wa")

for i in labeled_meta_DF.T.to_dict().values():
    simplejson.dump(i, foutput_meta)
    foutput_meta.write('\n')
