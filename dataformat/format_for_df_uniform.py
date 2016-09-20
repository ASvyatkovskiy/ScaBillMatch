#!/usr/bin/env python
import simplejson

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
        'WY': 'Wyoming'
}

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

from collections import defaultdict

reverse_states = defaultdict(list)
for key, value in states.items():
    reverse_states[value].append(key)

data = open("../data/uniform_2.txt").readlines()
#Unsworn Foreign Declarations Act (2008) Colorado        HB 1190 2009    Levy    Enacted

output_dicts_combined = list()
for line in data:
    if 'VirginIslands' in line or 'PuertoRico' in line or 'DistrictofColumbia' in line: continue 
    parsed = line.split() 

    #"Adult Guardianship and Protective Proceedings Jurisdiction Act  RhodeIsland     HB7687/SB2548   2014    Craven/McCaffrey        Introduced"
    if '/' in line:
        #Adult Guardianship and Protective Proceedings Jurisdiction Act  RhodeIsland     HB7687/SB2548   2014    Craven/McCaffrey        Introduced
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
        output_dict = {'year':None, 'state':'','docid':'', 'docversion':'', 'primary_key':''}
        output_dict['year'] = int(year)
        output_dict['state'] = us_state_abbrev[state]
        output_dict['docid'] = bill
        output_dict['docversion'] = docversion
        output_dict['primary_key'] = state+"_"+year+"_"+bill+"_"+docversion
        output_dicts_combined.append(output_dict)

unique_output_dicts_combined = {each['primary_key'] : each for each in output_dicts_combined}.values()

foutput_combined = open("/scratch/network/alexeys/bills/lexs/bills_uniform.json","wa")
for i, output_dict in enumerate(unique_output_dicts_combined):
    #if not use_cryptic_pk: output_dict['primary_key'] = i
    simplejson.dump(output_dict, foutput_combined)
    foutput_combined.write('\n')
