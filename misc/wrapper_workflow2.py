#!/usr/bin/env python

from subprocess import Popen

Popen("sbt assembly",shell=True).wait()
Popen("spark-submit --class org.apache.spark.ml.feature.ExtractMinHashLSH --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True)

Popen("spark-submit --class org.princeton.billmatch.utils.HarvestOutput --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True)

config = open("src/main/resources/workflow2.conf").readlines()
for line in config:
    if "outputFileBase" in line: outputFileBase = line.split("=").lstrip().rstrip(",")+"_*"
Popen("hdfs dfs -rmr "+outputFileBase,shell=True).wait()
