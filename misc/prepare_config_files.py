#!/usr/bin/env python


def createConfigs(configBaseName, nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders):
    def createOneConfig(nFolders):
        if nFolders < 0: return
        with open(configBaseName+str(nFolders)+".conf","w") as f:
            f.write(configBaseName+" {\n")
            f.write("  nPartitions  = "+str(nPartitions)+",\n")
            f.write("  nCPartitions = "+str(nCPartitions)+",\n")
            f.write("  measureName = \""+measureName+"\",\n")
            f.write("  inputParquetFile = \""+inputParquetFile+"\",\n")
            f.write("  inputPairsFile = \""+inputPairsFile+"/p"+str(nFolders)+"\",\n")
            f.write("  outputMainFile = \""+outputMainFile+"_"+str(nFolders)+"\",\n")
            f.write("}\n")
        createOneConfig(nFolders-1)
    createOneConfig(nFolders)

if __name__=='__main__':
    nFolders = 14
    configBaseName = "billAnalyzer"
    nPartitions = 120
    nCPartitions = 3000
    measureName = "jaccard"
    inputParquetFile = "/user/alexeys/bills_combined3_5gram"
    inputPairsFile = "/user/alexeys/valid_pairs3_5gram"
    outputMainFile = "/user/alexeys/output3_5gram"
    createConfigs(configBaseName, nPartitions, nCPartitions, measureName, inputParquetFile, inputPairsFile, outputMainFile, nFolders)
