#Step 0: prepare combined JSON file

We used to prepare the bill.json and bills_metadata.json structured like this:

```bash
{"name": "year", "type": "int"},
{"name": "state",  "type": "string"},
{"name": "docid", "type": "string"},
{"name": "docversion", "type": "string"},
{"name": "primary_key", "type": "string"}
```

and that:

```bash
{"name": "content", "type": "string"},
{"name": "primary_key", "type": "string"}
```

Which is now going to be just that:

```bash
{"name": "year", "type": "int"},
{"name": "state",  "type": "string"},
{"name": "docid", "type": "string"},
{"name": "docversion", "type": "string"},
{"name": "content", "type": "string"},
{"name": "primary_key", "type": "string"}
```

Motivation: we can access only the columns we need by specifying JSON schema like that in the code:

```scala
val jsonSchema = spark.read.json(spark.sparkContext.parallelize(Array("""{"docversion": "str", "docid": "str", "primary_key": "str", "content": "str"}""")))
val input = spark.read.format("json").schema(jsonSchema.schema).load("path/to/input/file")
```

And can be achieved by running the Python script:

```bash
cd dataformat/
python format_for_df.py
```

make sure to specify path to input files for `glob.glob` to find them (as usual).

For 11 states it took less than 10 minutes (did not time precisely).

# Step 1: make cartesian step

This is now called `MakeLabeledCartesian`. The difference is, we add the k-means clustering step
for preprocessing to extract labels and to group the input data to groups which can be close (clusters). Therefore, we
identify candidate pairs and do not waste time considering pairs which are far away. 

Update the resource input file to something like this:
```scala
makeCartesian {
  nPartitions  = 30,
  docVersion   = "Introduced",
  onlyInOut    = false,
  inputFile    = "file:///scratch/network/alexeys/bills/lexs/bills_combined_3.json",
  outputFile   = "/user/alexeys/valid_pairs_3_kMeans",
  use_strict   = false,
  strict_state = 8,
  strict_docid = "SB436",
  strict_year  = 2005
  numTextFeatures = 16384,
  addNGramFeatures = false,
  nGramGranularity = 2
}
```

Submit to cluster:

```bash
spark-submit --class MakeLabeledCartesian --master yarn --queue production --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

Run summary for 3 states:

```bash
== Physical Plan ==
Within Set Sum of Squared Errors = 2.0737994190916973E10
{
	kmeans_6ea773dd0a64-featuresCol: features,
	kmeans_6ea773dd0a64-initMode: k-means||,
	kmeans_6ea773dd0a64-initSteps: 5,
	kmeans_6ea773dd0a64-k: 150,
	kmeans_6ea773dd0a64-maxIter: 40,
	kmeans_6ea773dd0a64-predictionCol: prediction,
	kmeans_6ea773dd0a64-seed: -1689246527,
	kmeans_6ea773dd0a64-tol: 1.0E-4
}

Elapsed time: 1485s
```


# Step2: similarity calculation

This step has not changed froim before at all. Only the class name has changed (AdhocAnalyzer is now our main, so let us call it a better name). In addition, minor changes have been made to migrate to Spark 2.0.0 and Scala 2.11.8.

Update the resource input file to something like this:

```scala
billAnalyzer {
  numTextFeatures = 16384,
  docVersion   = "Introduced",
  addNGramFeatures = false,
  nGramGranularity = 2,
  measureName = "cosine",
  inputBillsFile = "file:///scratch/network/alexeys/bills/lexs/bills_combined_3.json",
  inputPairsFile = "/user/alexeys/valid_pairs_3_kMeans",
  outputMainFile = "/user/alexeys/output_cosine_3"
}
```
And run the job like:

```bash
spark-submit  --class BillAnalyzer --master yarn --queue production --num-executors 40 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

For 3 states, run took:
```bash
Elapsed time: 4855s
```

#Step 3: histogramming and plotting

Once the output is produced, one can perfrom histogramming and plotting using spark-shell and histogrammar packages.
Start the spark-shell as follows:

```bash
spark-shell --master yarn --queue production --num-executors 30 --executor-cores 3 --executor-memory 5g --jars target/scala-2.11/BillAnalysis-assembly-2.0.jar
```

First, print top similarity matches for this dataset, selecting a probe bill of interest:

```scala
val data = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_cosine_3").cache()
val filtered_data = data.filter({case ((k1,k2),v) => ((k1 contains "A_BILL_OF_INTEREST") || (k2 contains "A_BILL_OF_INTEREST"))}).cache()

val sorted_data = filtered_data.map(x => x.swap).sortByKey(false)
for (s <- sorted_data.take(100)) {
    println(s)
}
```

Filtering ans sorting will take a while, so do not calculate all-against-all if you are just looking for some diffusion bill!


# Inputs and outputs used for test

Inputs:

```bash
/scratch/network/alexeys/bills/lexs/bills_combined_3.json
```

```bash
/user/alexeys/valid_pairs_3_kMeans
/user/alexeys/output_cosine_3
```
