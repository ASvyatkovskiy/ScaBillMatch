#!/usr/bin/env python

from subprocess import Popen

for comb in range(14):
    Popen("cp billAnalyzer"+str(comb)+".conf src/main/resources/billAnalyzer.conf",shell=True).wait()
    Popen("sbt assembly",shell=True).wait()
    Popen("spark-submit --class org.princeton.billmatch.BillAnalyzer --master yarn --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True).wait()
