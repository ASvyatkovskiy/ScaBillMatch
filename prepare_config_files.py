#!/usr/bin/env python
import sys
from pyhocon import ConfigFactory

def createConfigsA(conf,nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders):
    def createOneConfig(conf,nFolders):
        #maybe assert for it
        configBaseName = "workflow1_billAnalyzer"
        if nFolders < 0: return
        with open(configBaseName+str(nFolders)+".conf","w") as f:
            f.write("workflow1_billAnalyzer {\n")
            f.write("  nPartitions  = "+str(nPartitions)+",\n")
            f.write("  nCPartitions = "+str(nCPartitions)+",\n")
            f.write("  measureName = \""+measureName+"\",\n")
            f.write("  inputParquetFile = \""+inputParquetFile+"\",\n")
            f.write("  inputPairsFile = \""+inputPairsFile+"/p"+str(nFolders)+"\",\n")
            f.write("  outputMainFile = \""+outputMainFile+"_"+str(nFolders)+"\",\n")
            f.write("}\n")
        createOneConfig(conf,nFolders-1)
    createOneConfig(conf,nFolders)

def createConfigsB(conf,nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, strictCats):
    def createOneConfig(conf):
        configBaseName,conf_items = conf.items() #"workflow1_makeCartesian" 
        with open(configBaseName+".conf","w") as f:
            f.write(configBaseName+" {\n")
            if cat != "":
                f.write("    cat = \""+cat+"\",\n")
            for k,v in conf_items: 
                f.write("    {}={},\n".format(k,v))
            f.write("}\n")
    createOneConfig(conf)

def createConfigsC(conf,cat,nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, strictCats):
    def createOneConfig(conf,cat):
        configBaseName,conf_items = conf.items() #"workflow1_makeCartesian" 
        with open(configBaseName+cat+".conf","w") as f:
            f.write(configBaseName+" {\n")
            if cat != "":
                f.write("    cat = \""+cat+"\",\n")
            f.write("    optimizationLevel = 0,\n")
            f.write("    docVersion   = \"Introduced\",\n")
            f.write("    useLSA       = false,\n")
            f.write("    onlyInOut    = true,\n")
            f.write("    nGramGranularity = 5,\n")
            f.write("    addNGramFeatures = true,\n")
            f.write("    inputFile    = \"file:///scratch/network/alexeys/bills/lexs/bills_combined_3_COILNJ_new.json\",\n")
            if cat != "":
                f.write("    outputFile   = \"/user/alexeys/valid_pairs_\""+cat+",\n")
                f.write("    outputParquetFile = \"/user/alexeys/bills_combined_\""+cat+",\n")
            else:
                f.write("    outputFile   = \"/user/alexeys/valid_pairs\",\n")
                f.write("    outputParquetFile = \"/user/alexeys/bills_combined\",\n")
            f.write("    nPartitions  = 40,\n")
            f.write("    numTextFeatures = 1048576,\n")
            f.write("    kval = 40,\n")
            f.write("    numConcepts = 1500\n")
            f.write("}\n")
    for cat in strictCats:
        createOneConfig(conf,cat)


if __name__=='__main__':
    base_config_path = 'src/main/resources/workflow1_makeCartesian.conf'
    conf = ConfigFactory.parse_file(base_config_path)

    nFolders = 14
    nPartitions = 120
    nCPartitions = 3000
    measureName = "maxasymjaccard"
    inputParquetFile = sys.argv[1] #/user/alexeys/bills_combined
    inputPairsFile = sys.argv[2] #/user/alexeys/valid_pairs
    outputMainFile = sys.argv[3] #/user/alexeys/output_sample
    createConfigsA(conf, nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders)
