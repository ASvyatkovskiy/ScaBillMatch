#Checking out the code

```bash
git clone https://github.com/ASvyatkovskiy/ScaBillMatch
cd ScaBillMatch
sbt assembly
```

This code relies on Scala, Spark Python
```
module load anaconda
module load spark/hadoop2.6/2.1.0
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


# Workflow2

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

# Workflow1

Proceeds in 2 steps

## Make pairs


## Clustering + similarity

# Postprocessing

```scala
import org.princeton.billmatch.stats._

//takes the output of the steps2 of the bill analysis (can either be workflow2 or workflow1)
val skimmed_data = AnalysisUtils.sampleNOrdered(spark,"file:///...json","/user/alexeys/.../parquet",50000,false,false)
//to get light specify:
//val skimmed_data = AnalysisUtils.sampleNOrdered(spark,"file:///...json","/user/alexeys/.../parquet",-1,false,true)

//imposes temporal order among key columns
val ordered_data = AnalysisUtils.imposeTemporalOrder(skimmed_data)

//saves to the skimmed
ordered_data.repartition(10).write.json("/user/alexeys/...")
```

