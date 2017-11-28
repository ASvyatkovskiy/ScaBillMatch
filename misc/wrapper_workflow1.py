#!/usr/bin/env python

from subprocess import Popen

Popen("sbt assembly",shell=True).wait()
Popen("spark-submit --class org.princeton.billmatch.ExtractCandidates --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True).wait()

Popen("python prepare_valid_pairs.py /user/alexeys/valid_pairs/",shell=True).wait()
Popen("python prepare_config_files.py /user/alexeys/bills_combined/ /user/alexeys/valid_pairs/ /user/alexeys/output_sample/",shell=True).wait()

for comb in range(14):
    Popen("cp billAnalyzer"+str(comb)+".conf src/main/resources/billAnalyzer.conf",shell=True).wait()
    Popen("sbt assembly",shell=True).wait()
    Popen("spark-submit --class org.princeton.billmatch.BillAnalyzer --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True).wait()
