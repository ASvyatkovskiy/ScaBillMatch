# Checking out the code

```bash
git clone https://github.com/ASvyatkovskiy/ScaBillMatch
cd ScaBillMatch
sbt assembly
```

This code relies on Scala 2.11, Spark 2.2, Python 2.7
```
module load anaconda
module load spark/hadoop2.6/2.2.0
```

#Preprocessing

Start by creating JSON files in the analysis format. JSON is not the only format that one can use -- Avro is a good alternative option,
which we leave out for now.

```bash
#cd ScaBillMatch
cd dataformat
python Preprocess.py
```

`Preprocessor.py` implements a class for converting raw text data into JSON format. It's constructor takes following parameters:

```python
class Preprocess(object):

    def __init__(self,inputname,outputname,use_cryptic_pk=True,exclude_uniforms=True):
```    

1. input_name: folder with input files
1. ouput_name: output file name
1. use_cryptic_pk: boolean flag switching between how you store primary_key for bills, state+year+docid+docversion concatenated or a unique integer
1. exclude_uniforms: self explanatory

It is recommended to split 50 state sample into 2 JSON files as the version on github suggests.


# Workflow2 [easy]

Access and edit the configuration file for Workflow2 (MinHash LSH):
```
workflow2 {
  docVersion   = "Introduced",
  nGramGranularity = 5,
  inputFile    = "file:///scratch/network/alexeys/bills/lexs/bills_combined_50_p*.json",
  outputFileBase   = "/user/alexeys/wf2_similarity_join",
  numTextFeatures = 1048576
}
```
`numTextFeatures` determines the maximum number of hash buckets for text frequency calculation.

One would need to recompile after updating the config parameters:
```
sbt assembly
```

Submit as follows:
```bash
spark-submit --class org.apache.spark.ml.feature.ExtractMinHashLSH --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar

#FIXME need one more step to combine output folders per state pair to 1 folder
#FIXME put all in wrapper_workflow2.py
```

The output will be a set of parquet files, one folder per state pair.

# Workflow1 [more difficult]

Proceeds in 2 steps

## Make pairs

`ExtractCandidates` produces all the pairs of primary keys of the documents satisfying a predicate. Performs document clustering using k-means.

Following are the key parameters that need to be filled in the resources/makeCartesian.conf file:
        docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted...
        useLSA: whether to use truncated SVD
        numConcepts: number of concepts to use for LSA
        kval: number of clusters for k-means
        onlyInOut: a switch between in-out of state and using both in-out and in-in state pairs
        inputFile: input file, one JSON per line
        outputFile: output file
        outputParquetFile: output parquet sink
        
Configure parameters for your run in `src/main/resources/workflow1_makeCartesian.conf`:
```bash
workflow1_makeCartesian {
  optimizationLevel = 1,
  docVersion   = "Introduced",
  useLSA       = false,
  onlyInOut    = true,
  nGramGranularity = 5,
  addNGramFeatures = true,
  inputFile    = "file:///scratch/network/alexeys/bills/lexs/bills_combined_50_p*.json",
  outputFile   = "/user/alexeys/valid_pairs_50",
  outputParquetFile = "/user/alexeys/bills_combined_50",
  nPartitions  = 40,
  numTextFeatures = 1048576,
  kval = 400,
  numConcepts = 100
}
```

Most of the parameters are self-explanatory. Here are some which are less clear:

1. optimizationLevel: 0 -- all pairs (also called baseline), 1 -- enable k-means (we call it workflow1), 2 -- not used in the paper: applies more aggressive selection on the bill lengths in a pair.

1. useLSA -- whether or not to apply truncated SVD, recommended set to false
1. onlyInOut -- setting this to true will only pair bills from different states
1. addNGramFeatures -- setting this to false will only use unigram features ("bag of words"). Note, that setting it to true will give pure n-gram feature rather than concatenating unigram and 5-gram (something one can do in scikit-learn) 
1. nGramGranularity -- setting to 5 will give 5gram, requires addNGramFeatures = true
1. nPartitions: do not change
1. numTextFeatures: number of buckets fro hashing TF, suggested not to reduce
1. numConcepts: only enabled if useLSA=true, determines the dimensionality after truncating SVD
1. kval: number of clusters for k-means, disabled when optimizationLevel=0


To run:

```bash
spark-submit --class org.princeton.billmatch.ExtractCandidates --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```


## Clustering + similarity

BillAnalyzer: an app. that performs document or section all=pairs similarity starting off CartesianPairs

Following are the key parameters need to be filled in the resources/billAnalyzer.conf file:
    measureName: Similarity measure used
    inputParquetFile: Parquet file with features
    inputPairsFile: CartesianPairs object input file
    outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[String,String],Double]

```
workflow1_billAnalyzer {
  nPartitions  = 120,
  nCPartitions = 3000,
  measureName = "maxasymjaccard",
  inputParquetFile = "/user/alexeys/bills_combined_militaryVoters/",
  inputPairsFile = "/user/alexeys/valid_pairs_militaryVoters/p10",
  outputMainFile = "/user/alexeys/output_militaryVoters_uniform_10",
}
```

The only fields you would have to change here are:
1. measureName: which similarity measure
1. inputParquetFile: should match `workflow1_makeCartesian.outputParquetFile`
1. inputPairsFile: should match `workflow1_makeCartesian.outputFile`
1. outputMainFile: output file name that you choose

To run:
```bash
spark-submit --class org.princeton.billmatch.BillAnalyzer --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

## Automate submission 

To automate the submission, use the `misc/wrapper_workflow1.py` script, which looks like this:

```python
#!/usr/bin/env python

from subprocess import Popen

#step1: prepares eligible pairs
Popen("sbt assembly",shell=True).wait()
Popen("spark-submit --class org.princeton.billmatch.ExtractCandidates --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True).wait()

#this expects the workflow1_billAnalyzer.inputPairsFile
Popen("python prepare_valid_pairs.py /user/alexeys/valid_pairs/",shell=True).wait()
#workflow1_billAnalyzer.inputParquetFile workflow1_billAnalyzer.inputPairsFile workflow1_billAnalyzer.outputMainFile
Popen("python prepare_config_files.py /user/alexeys/bills_combined/ /user/alexeys/valid_pairs/ /user/alexeys/output_sample/",shell=True).wait()

#runs bill analysis step2 as 14 jobs
for comb in range(14):
    Popen("cp billAnalyzer"+str(comb)+".conf src/main/resources/billAnalyzer.conf",shell=True).wait()
    Popen("sbt assembly",shell=True).wait()
    Popen("spark-submit --class org.princeton.billmatch.BillAnalyzer --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar",shell=True).wait()
    
#FIXME need one more step to convert object files to parquet    
```

To run:

```bash
python wrapper_workflow1.py
```

## Baseline

Note, that the steps are the same for Workflow1 and baseline. Only one needs to set the 

```bash
optimizationLevel=0,
```
for baseline.

# Postprocessing

We use 3 different formats for analysis:
1. Raw: raw output in parquet format. Following schema:
```
scala> data.printSchema
root
 |-- pk1: string (nullable = true)
 |-- pk2: string (nullable = true)
 |-- similarity: double (nullable = true)

```
1. Skim: JSON format, top N pairs, following schema:
```
scala> data.printSchema
root
 |-- content1_smaller: string (nullable = true)
 |-- content2_larger: string (nullable = true)
 |-- pk1_smaller: string (nullable = true)
 |-- pk2_larger: string (nullable = true)
 |-- similarity: double (nullable = true)
```
1. Light: JSON, all pairs. Following schema:
```
scala> data.printSchema
root
 |-- pk1_smaller: string (nullable = true)
 |-- pk2_larger: string (nullable = true)
 |-- similarity: double (nullable = true)
```


## Produce skim

Launch the spark-shell like:
```bash
spark-shell --master yarn --queue production --num-executors 40 --executor-cores 3 --executor-memory 15g --driver-memory 20g --conf "spark.shuffle.memoryFraction=0.7" --conf "spark.shuffle.service.enabled=true" --jars target/scala-2.11/BillAnalysis-assembly-2.0.jar --packages "org.diana-hep:histogrammar-bokeh_2.11:1.0.4" --conf "spark.driver.maxResultSize=20g"
```

Then type:
```scala
import org.princeton.billmatch.stats._

//takes the initial data in JSON format and the output of the steps2 of the bill analysis (can either be workflow2 or workflow1)
val skimmed_data = AnalysisUtils.sampleNOrdered(spark,"file:///...json","/user/alexeys/.../parquet",50000,false,false)

//imposes temporal order among key columns
val ordered_data = AnalysisUtils.imposeTemporalOrder(skimmed_data)

//saves to the skimmed
ordered_data.repartition(10).write.json("/user/alexeys/...")
```


## Produce light
```
import org.princeton.billmatch.stats._

//to get light specify:
val skimmed_data = AnalysisUtils.sampleNOrdered(spark,"file:///...json","/user/alexeys/.../parquet",-1,false,true)

//imposes temporal order among key columns
val ordered_data = AnalysisUtils.imposeTemporalOrder(skimmed_data)

//saves to the skimmed
ordered_data.repartition(10).write.json("/user/alexeys/...")
```


# LDA

