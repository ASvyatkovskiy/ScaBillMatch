#!/usr/bin/env python

import simplejson
import glob
import sys,os,re

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
    'WY':49,
    'FD':50
}

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'AmericanSamoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'DistrictofColumbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'NorthernMarianaIslands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'NorthCarolina',
        'ND': 'NorthDakota',
        'NE': 'Nebraska',
        'NH': 'NewHampshire',
        'NJ': 'NewJersey',
        'NM': 'NewMexico',
        'NV': 'Nevada',
        'NY': 'NewYork',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'PuertoRico',
        'RI': 'RhodeIsland',
        'SC': 'SouthCarolina',
        'SD': 'SouthDakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'U.S.VirginIslands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'WestVirginia',
        'WY': 'Wyoming',
        'FD': 'Federal'
}

class Preprocess(object):

    def __init__(self,input_base_dir,output_base_dir,ofilename,use_cryptic_pk=True,exclude_uniforms=True,data_in_lexis_format=False):
        #switch between primary key as a long integer and primary key as a string
        self.use_cryptic_pk = use_cryptic_pk
        self.input_base_dir = input_base_dir
        self.output_file_name = ofilename
        self.output_base_dir = output_base_dir
        self.output_dicts = list()
        self.exclude_uniforms = exclude_uniforms
        self.data_in_lexis_format = data_in_lexis_format

    def fillStructures(self,state="all"): 
        if self.data_in_lexis_format: self.convert_from_lexis(self.input_base_dir,self.output_base_dir,state)
        if state == "all":
            inputs = glob.glob(self.output_base_dir+"/catalog_*")
        else:
            inputs = glob.glob(self.output_base_dir+"/catalog_"+state+"*")

        if self.exclude_uniforms: uniforms = self.fillUniforms()
        for input in inputs:
            with open(input,mode="r") as finput:
                lines = finput.readlines()
                for line in lines:
                    sid, content = line.split("^^^")
                    state, year, docid, docversion_pre = sid.split("/")

                    output_dict = {'content':None, 'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':'','length':0}
                    output_dict['year'] = int(year)
                    output_dict['state'] = us_state_abbrev[state]
                    output_dict['docid'] = docid
                    version = docversion_pre.rstrip(".txt")
                    #version = "".join(version.split())
                    output_dict['docversion'] = version
                    output_dict['primary_key'] = state+"_"+year+"_"+docid+"_"+version
                    content = content.decode("utf-8",errors='replace')
                    if state != "FD":
                        content = content.rpartition('TEXT: ')[-1]
                    else:
                        content = content.rpartition('SECTION 1.')[-1]
                    output_dict['content'] = content.partition('SPONSOR: ')[0]
                    output_dict['length'] = len(output_dict['content'])
                    if self.exclude_uniforms:
                        if output_dict['primary_key'] not in uniforms: self.output_dicts.append(output_dict)
                    else:
                        self.output_dicts.append(output_dict)

        #assert unique
        self.output_dicts  = {each['primary_key'] : each for each in self.output_dicts}.values()

    def saveStructures(self):
        o = os.path.join(self.output_base_dir,self.output_file_name)
        if os.path.isfile(o):
            mode = "a+"
        else: mode = "w+"

        with open(o,mode) as output_file_name:
            for i, output_dict in enumerate(self.output_dicts):
                if not self.use_cryptic_pk: output_dict['primary_key'] = i
                simplejson.dump(output_dict, output_file_name)
                output_file_name.write('\n')


    def convert_from_lexis(self,base_hdfs_dir,output_base_dir,state="all"):
        '''Concert plain text files from LexisNexis into
         document_id, document contents as string a single files'''
        import pandas as pd
        from subprocess import Popen

        if state == "all":
            billWalkOpj =  os.walk(os.path.join(base_hdfs_dir))
        else:
            billWalkOpj =  os.walk(os.path.join(base_hdfs_dir,state))
        for (billdirpath, billdirnames, billfilenames) in billWalkOpj:
            fname = ''

            for bfname in billfilenames:
                (bfn, bftype) = os.path.splitext(bfname)
                if bftype != ".txt":
                    continue
                #print output_base_dir+"/catalog"+str(state)+str(year)
                billFilePath = '%s/%s' % (billdirpath,bfname)
                if state == "FD" or state == "federal":
                    if not "text-versions/ih" in billdirpath and not "text-versions/is" in billdirpath: continue
                    #print "/".join([billdirpath,"data.json"])
                    metadata = pd.read_json("/".join([billdirpath,"data.json"]))
                    docid = metadata['bill_version_id']['html']
                    year = metadata['issued_on']['html'].split("-")[0]
                    sid = "{}/{}/{}/whatever_Introduced.txt".format(state,year,docid)
                else:
                    state = billdirpath.split('/')[-3] 
                    version = "".join(billFilePath.split("/")[-1].split("_")[1:])
                    year = billdirpath.split('/')[-2]
                    docid = billdirpath.split("/")[-1]
                    sid = "{}/{}/{}/{}".format(state,year,docid,version)

                #print billFilePath
                data=open(billFilePath).read().replace('\n', ' ')
                fname = "catalog_"+str(state)+str(year)
                data_tuple = sid+"^^^"+data+"\n"
                fname = os.path.join(output_base_dir,fname)
                if not os.path.isfile(fname):
                    ofile = open(fname,"wb")
                    ofile.write(data_tuple)
                    ofile.close()
                else: 
                    ofile = open(os.path.join(output_base_dir,fname),"ab") 
                    #ofile.seek(0, 2)
                    ofile.write(data_tuple)
                    ofile.close()
            #if os.path.isfile(fname):
            #    Popen("mv "+fname+" "+output_base_dir,shell=True).wait()
            #    Popen("rm "+fname,shell=True).wait() 


    def fillUniforms(self):

        #get a dictionary for reverse state lookup
        from collections import defaultdict
        reverse_states = defaultdict(list)
        for key, value in states.items():
            reverse_states[value].append(key)

        uniforms = open("../data/uniform.txt").readlines()

        #output_dicts_uniforms = list()
        output_dicts_uniforms = set()
        for line in uniforms:
            if 'VirginIslands' in line or 'PuertoRico' in line or 'DistrictofColumbia' in line: continue
            parsed = line.split()

            if '/' in line:
                try:
                    state, bills, year, f, docversion = parsed[-5:]
                    state = reverse_states[state][0]
                    bills = bills.split("/")
                except:
                    state, bills, year, docversion = parsed[-4:]
                    state = reverse_states[state][0]
                    bills = bills.split("/")
            else:
                try:
                    state, billID, billNum, year, f, docversion = parsed[-6:]
                    state = reverse_states[state][0]
                    bills = [billID+billNum]

                except:
                    state, billID, billNum, year, docversion = parsed[-5:]
                    state = reverse_states[state][0]
                    bills = [billID+billNum]

            for bill in bills:
                #output_dict = {'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':''}
                #output_dict['year'] = int(year)
                #output_dict['state'] = us_state_abbrev[state]
                #output_dict['docid'] = bill
                #output_dict['docversion'] = docversion
                #output_dict['primary_key'] = state+"_"+year+"_"+bill+"_"+docversion
                output_dicts_uniforms.add(state+"_"+year+"_"+bill+"_"+docversion)

            #output_dicts_uniforms = {each['primary_key'] : each for each in output_dicts_uniforms}.values()
        return output_dicts_uniforms


if __name__=='__main__':

    for state in ['CO','IL','NJ']:
        preprocessor = Preprocess("/scratch/network/alexeys/raw_bills","/scratch/network/shiraito/text/diffusion/replication/preprocessed/","test_COILNJ.json",True,True,True)
        preprocessor.fillStructures(state)
        preprocessor.saveStructures()
