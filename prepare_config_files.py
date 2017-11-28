#!/usr/bin/env python
import sys

def createConfigsA(configBaseName, nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders):
    def createOneConfig(nFolders):
        if nFolders < 0: return
        with open(configBaseName+str(nFolders)+".conf","w") as f:
            f.write("billAnalyzer {\n")
            f.write("  nPartitions  = "+str(nPartitions)+",\n")
            f.write("  nCPartitions = "+str(nCPartitions)+",\n")
            f.write("  measureName = \""+measureName+"\",\n")
            f.write("  inputParquetFile = \""+inputParquetFile+"\",\n")
            f.write("  inputPairsFile = \""+inputPairsFile+"/p"+str(nFolders)+"\",\n")
            f.write("  outputMainFile = \""+outputMainFile+"_"+str(nFolders)+"\",\n")
            f.write("}\n")
        createOneConfig(nFolders-1)
    createOneConfig(nFolders)

def createConfigsB(nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, strictCats):
    def createOneConfig(cat):
        configBaseName = "makeCartesian" 
        with open(configBaseName+cat+".conf","w") as f:
            f.write(configBaseName+" {\n")
            if cat != "":
                f.write("    cat = \""+cat+"\",\n")
            f.write("    optimizationLevel = 1,\n")
            f.write("    docVersion   = \"Introduced\",\n")
            f.write("    useLSA       = false,\n")
            f.write("    onlyInOut    = true,\n")
            f.write("    nGramGranularity = 5,\n")
            f.write("    addNGramFeatures = true,\n")
            f.write("    inputFile    = \"file:///scratch/network/alexeys/bills/lexs/bills_combined_50_p*.json\",\n")
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
        createOneConfig(cat)


if __name__=='__main__':
    nFolders = 14
    nPartitions = 120
    nCPartitions = 3000
    measureName = "maxasymjaccard"
    inputParquetFile = sys.argv[1] #"/user/alexeys/bills_combined50_300k_replica2_unigram"
    inputPairsFile = sys.argv[2] #"/user/alexeys/valid_pairs_50_300k_replica2_unigram"
    outputMainFile = sys.argv[3] #"/user/alexeys/output50_300k_replica2_unigram"
    configBaseName = sys.argv[4]
    createConfigsA(configBaseName, nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders)
