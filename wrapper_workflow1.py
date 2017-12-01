#!/usr/bin/env python

import sys,os,getpass
from subprocess import Popen


base_hdfs_path ="/user"
jar_path = "target/scala-2.11/BillAnalysis-assembly-2.0.jar"

Popen("sbt assembly",shell=True).wait()
Popen("spark-submit --class org.princeton.billmatch.ExtractCandidates --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g {}".format(jar_path),shell=True).wait()

Popen("python prepare_valid_pairs.py {}".format(os.path.join(base_hdfs_path,getpass.getuser(),"valid_pairs")),shell=True).wait()
Popen("python prepare_config_files.py {} {} {}".format(os.path.join(base_hdfs_path,getpass.getuser(),"bills_combined"), os.path.join(base_hdfs_path,getpass.getuser(),"valid_pairs"),os.path.join(base_hdfs_path,getpass.getuser(),"output_sample"))

for comb in range(14):
    Popen("cp workflow1_billAnalyzer"+str(comb)+".conf src/main/resources/workflow1_billAnalyzer.conf",shell=True).wait()
    Popen("sbt assembly",shell=True).wait()
    Popen("spark-submit --class org.princeton.billmatch.BillAnalyzer --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g {}".format(jar_path),shell=True).wait()

#run harvester changing app=workflow1
Popen("spark-submit --class org.princeton.billmatch.utils.HarvestOutput --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g {}".format(jar_path),shell=True).wait()

#cleanup folders
config = open("src/main/resources/workflow1_billAnalyzer.conf","r").readlines()
for line in config:
    if "outputMainFile" in line: outputMainFile = line.split("=")[-1].lstrip(" \"").rstrip("\",\n")
outputMainFile = "_".join(outputMainFile.split("_")[:-1])+"_*"
Popen("hdfs dfs -rmr "+outputMainFile,shell=True).wait()
