#!/usr/bin/env python

import simplejson
import glob

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

    def __init__(self,inputname,outputname,use_cryptic_pk=True,exclude_uniforms=True):
        #switch between primary key as a long integer and primary key as a string
        self.use_cryptic_pk = use_cryptic_pk
        self.inputs = glob.glob(inputname) #"/scratch/network/alexeys/bills/lexs/text_10states/*/*/catalog_*")
        self.output_file = open(outputname,"wa") #"/scratch/network/alexeys/bills/lexs/bills_combined_10.json","wa")
        self.output_dicts = list()
        self.exclude_uniforms = exclude_uniforms

    def fillStructures(self): 
        if self.exclude_uniforms: uniforms = self.fillUniforms()
        for input in self.inputs:
            with open(input,mode="r") as finput:
                lines = finput.readlines()
                for line in lines:
                    sid, content = line.split("^^^")
                    split_sid = sid.split("/")
                    if len(split_sid) == 5: 
                        bla, state, year, docid, docversion_pre = split_sid
                    elif len(split_sid) == 6:
                        blah, bla, state, year, docid, docversion_pre = split_sid
                    else: 
                        print "Tokenization issue in file: ", input
                        break
 
                    output_dict = {'content':None, 'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':''}
                    output_dict['year'] = int(year)
                    output_dict['state'] = us_state_abbrev[state]
                    output_dict['docid'] = docid
                    version = docversion_pre.split("_")[1].rstrip(".txt")
                    version = "".join(version.split())
                    output_dict['docversion'] = version
                    output_dict['primary_key'] = state+"_"+year+"_"+docid+"_"+version
                    content = content.decode("utf-8",errors='replace')
                    content = content.rpartition('TEXT: ')[-1]
                    output_dict['content'] = content.partition('SPONSOR: ')[0]
                    if self.exclude_uniforms:
                        if output_dict['primary_key'] not in uniforms: self.output_dicts.append(output_dict)
                    else:
                        self.output_dicts.append(output_dict)

        #assert unique
        self.output_dicts  = {each['primary_key'] : each for each in self.output_dicts}.values()

    def saveStructures(self):
        for i, output_dict in enumerate(self.output_dicts):
            if not self.use_cryptic_pk: output_dict['primary_key'] = i
            simplejson.dump(output_dict, self.output_file)
            self.output_file.write('\n')


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
    #preprocessor = Preprocess("/scratch/network/alexeys/bills/lexs/text_10states/*/*/catalog_*","/scratch/network/alexeys/bills/lexs/bills_combined_10.json",True,True)
    preprocessor = Preprocess("/scratch/network/alexeys/bills/lexs/text_50states_part2/*/*/catalog_*","/scratch/network/alexeys/bills/lexs/bills_combined_51.json",True,True)
    preprocessor.fillStructures()
    preprocessor.saveStructures()
