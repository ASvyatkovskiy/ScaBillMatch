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
```

The output will be a set of parquet files, one folder per state pair.

## Automatic submission

Simply do:
```
python wrapper_workflow2.py
```

And pick up your results in the raw format in the file pointed to by:
```
outputFileBase   = "/user/alexeys/wf2_similarity_join"
```
when it is finished. The proceed to "Postprocessing" section.

# Workflow1 [more convoluted]

Proceeds in 2 steps

## Make pairs

`ExtractCandidates` produces all the pairs of primary keys of the documents satisfying a predicate. Performs document clustering using k-means.
    
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
  numConcepts = 100,
  cat = ""
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

To automate the submission, use the `wrapper_workflow1.py` script.

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

## Baseline for a given diffusion topic

Sometime, we want calculate a similarity between all possible bill pairs where at least one bill belongs to a certain diffusion category determined by the researcher.

To accomplish that, one can prepare a plain text file with the list of bill identifier in the format: two letter state, 4 digits year, bill identifier, bill version, one value per line. For instance:

```
AK_2005_SB200_Introduced
GA_2005_SB396_Introduced
...
```
Note, that this is the data format for bills used throughout all the classes in this software tool.

Next, modify the configuration file`src/main/resources/workflow1_makeCartesian.conf` by adding a relative path to the plain text file containing the bill identifiers (parameter `cat`):
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
  numConcepts = 100,
  cat = "data/standYourgroundBills.txt"
}
```

What will happen is, a string with a predicate condition will be created and a filter will be applied before broadcasting the dataset during all pairs simialrity calculation. As a result, only bill pairs having at least one bill from that list will make it to the results.

Note: if you point to a non-existent file the job will terminate with a `FileNotFoundException`.

Note: the default value of the `cat` parameter is an empty string, it should be kept that way if your intension is to include all the bills in the dataset pointed to by `inputFile` string in the calculation.

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


## Produce all postprocessed files in one submission

```bash
spark-submit --class org.princeton.billmatch.utils.Postprocessor --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

This is configured in one conf file `src/main/resources/postprocessor.conf`:
```
postprocessor {
  inputJsonPath = "file:////scratch/network/alexeys/bills/lexs/bills_combined_3_COILNJ_new.json",
  inputResultParquet = "/user/alexeys/workflow2/raw/51/output_5gram_minhash_d0.99_buckets100_1048576_max_withcompacts",
  getNBest = 50000,
  isAscending = false,
  outputSkimFile = "/user/alexeys/output_skimmed",
  outputLightFile = "/user/alexeys/output_light"
}
```

Which takes the initial JSOn files and the output of the Workflow1 or Workflow2 in parquet format. This will produce both skim and light formats.

## If you only need light

If you only need light, you might be better off just doing it in spark-shell:
```scala
import org.princeton.billmatch.stats._

//to get light specify
val raw = spark.read.parquet(<path to output file of workflow1 or workflow2>) 
val ordered_light_data = AnalysisUtils.makeLight(raw)

//saves to the light
ordered_light_data.write.json(<name of the output file you give it>)
```


# LDA

To configure an LDA run, open `src/main/resources/ldaAnalyzer.conf`:

```
ldaAnalyzer {
  verbose = false,
  docVersion      = "Introduced",
  inputFile    = "file:///scratch/network/alexeys/bills/lexs/bills_combined_3_COILNJ_small.json",
  outputFile      = "/user/alexeys/LDA_CAFLVA_50t_unigram",
  nGramGranularity = 5,
  addNGramFeatures = false,
  kval = 30,
  numTextFeatures = 1048576,
  vocabSizeLimit = 2097152
}
```

Following parameters affect the feature extraction step (same as above):
1. addNGramFeatures: boolean flag to switch between unigram and n-gram
1. nGramGranularity: integer fixing the n-gram granularity size
1. numTextFeatures: number of hashing buckets for TF -- currently obsolete, as the count vectorizer is used as default
1. vocabSizeLimit: integer, power of 2, maximum size of the text frequency dictionary (CountVectorizer)

Affecting clustering:
1. kval: integer, number of clusters for LDA

Input/output paths:
1. inputFile: input JSON
1. outputFile: output parquet

To launch a job:
```bash
spark-submit --class org.princeton.billmatch.LDAAnalyzer --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

# Producing Metadata

## For federal

The data format is different between the federal and state bills. For federal house one can do:

```scala
//To produce Federal metadata for House bills only
scala> import java.io._

scala> val data = sc.wholeTextFiles("file:///scratch/network/alexeys/bills/lexs/federal/*/bills/h*/*/text-versions/ih*/data.json")
data: org.apache.spark.rdd.RDD[(String, String)] = file:///scratch/network/alexeys/bills/lexs/federal/*/bills/h*/*/text-versions/ih*/data.json MapPartitionsRDD[12] at wholeTextFiles at <console>:27

scala> val d = data.map(x=>x._2).map(x => x.replace("\n", "")).map(x => x.trim().replaceAll(" +", " "))
d: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[15] at map at <console>:29

//this makes a strign delimeted by end of line characters
scala> val ddd = d.collect().mkString("\n")

scala> val pw = new PrintWriter(new File("FD_house.json" ))

scala> pw.write(ddd)
scala> pw.close()
```

For federal senate bills, the base path would be:
```bash
file:///scratch/network/alexeys/bills/lexs/federal/*/bills/s*/*/text-versions/is*/data.json
```

Alternatively, one can use the MetadataConverter class:
https://github.com/ASvyatkovskiy/ScaBillMatch/blob/master/src/main/scala/org/princeton/billmatch/utils/MetadataConverter.scala

and configure it via: https://github.com/ASvyatkovskiy/ScaBillMatch/blob/master/src/main/resources/metadata.conf

```
metadata {
  inputJsonBasePath = "file:////scratch/network/alexeys/raw_bills",
  outputJsonBasePath = "/home/alexeys/PoliticalScienceTests/ScaBillMatch",
  isFederal = false
}
```

And run on the cluster (only yarn client deploy mode or local master will work!):
```bash
spark-submit --class org.princeton.billmatch.utils.MetadataConverter --master yarn --deploy-mode client --queue production --num-executors 40 --executor-cores 3 --executor-memory 16g --driver-memory 20g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```
