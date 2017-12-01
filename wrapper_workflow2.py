#!/usr/bin/env python

import sys,os,getpass
from subprocess import Popen

jar_path = "target/scala-2.11/BillAnalysis-assembly-2.0.jar"

Popen("sbt assembly",shell=True).wait()
Popen("spark-submit --class org.apache.spark.ml.feature.ExtractMinHashLSH --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g {}".format(jar_path),shell=True).wait()

Popen("spark-submit --class org.princeton.billmatch.utils.HarvestOutput --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g {}".format(jar_path),shell=True).wait()

config = open("src/main/resources/workflow2.conf","r").readlines()
for line in config:
    if "outputFileBase" in line: outputFileBase = line.split("=")[-1].lstrip(" \"").rstrip("\",\n")

Popen("hdfs dfs -rmr "+outputFileBase+"*_*",shell=True).wait()
print("The workflow2 results are in folder {}".format(outputFileBase))
