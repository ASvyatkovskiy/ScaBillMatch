#!/usr/bin/env python

from subprocess import Popen
import glob
from math import ceil
import sys,os

BASE_VALID_DIR = sys.argv[1]
FILES_PER_FOLDER = 3

#Prepare sub-folder
fileNames = glob.glob(os.path.join(BASE_VALID_DIR,"part*"))
totalFiles = len(fileNames)

print "Found total {} files".format(totalFiles) 
nFolders = int(ceil((totalFiles+FILES_PER_FOLDER)/FILES_PER_FOLDER))


print "Creating {} folders...".format(nFolders)
for i in range(nFolders):
    Popen("hdfs dfs -mkdir "+os.path.join(BASE_VALID_DIR,"p"+str(i)),shell=True).wait()


while fileNames:
    nFolders -= 1
    for _ in range(FILES_PER_FOLDER):
        try: 
            n = fileNames.pop(0)
            print "hdfs dfs -mv "+n+" "+os.path.join(BASE_VALID_DIR,"p"+str(nFolders))
            Popen("hdfs dfs -mv "+n+" "+os.path.join(BASE_VALID_DIR,"p"+str(nFolders)),shell=True).wait()
        except IndexError: pass         
