# ScaBillMatch [![Build Status](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)

Policy diffusion occurs when government decisions in a given jurisdiction are systematically influenced by prior policy choices made in other jurisdictions [Gilardi]. While policy diffusion can manifest in a variety of forms, we focus on a
type of policy diffusion that can be detected by examining similarity of legislative bill texts. We aim to identify groups of legislative bills from different states falling into the same diffusion topic, to perform an all-pairs comparison between the bills within each topic, and to identify paths connecting specific legislative proposals on a graph.


## Data ingestion

During ingestion step the raw unstructured data are converted into JSON and, subsequently, Apache Avro format having following schema:

```json
{"namespace" : "bills.avro" ,
   "type": "record",
   "name": "Bills",
   "fields": [
      {"name": "primary_key" , "type": "string"},
      {"name": "content" , "type" : "string"}
      {"name": "year" , "type" : "int"},
      {"name": "state" , "type" : "int"},
      {"name": "docversion" , "type" : "string"}
      ]
}
```

where the `primary_key` field is a unique identifier of the elements in the dataset constructed from year, state and
document version. The year, state and docversion fields are used to construct predicates and filter the data before the allpairs
similarity join calculation. The `content` field stores the entire legislative proposal as a unicode string. It is only used for feature extraction step, and is not read into memory during candidate selection and filtering steps, thanks to the Avro schema evolution property. 

Avro schema is stored in a file along with the data. Thus, if the program reading the data expects a different schema this can be easily resolved by setting the `avro.input.schema.key` in the Spark application, since the schemas of Avro writer and reader are both present.

The data ingestion steps would differ depending on the dataset structure/type.




## Pre-processing and feature extraction

The feature extraction step consists of a sequence of `Spark ML` transformers intended to produce numerical feature vectors
as a dataframe column. The resulting dataframe is fed to Spark ML k-means estimator, later used to calculate the all-pairs join, and subsequently during the graph analysis step with `GraphFrames`.




## Candidate aselection and clustering  

On the first step, a set of eligible candidate pairs is evaluated. This can either be all distinct combinatorial pairs of documents in a corpus (all-against-all), or set of pairs satisfying some stricter selection requirements in addition (one-against-all). 

`MakeCartesian` class will produce all the pairs of primary keys of the documents satisfying a predicate.
Following parameters need to be filled in the `resources/makeCartesian.conf` file:
* docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted... Leaving it an empty string will take all of the versions into account.
* nPartitions: number of partitions in bills_meta RDD
* use_strict: boolean, yes or no to consider stricter selection
* strict_state: specify state (a long integer from 1 to 50)
* strict_docid: specify document ID for one-against-all user selection (for instance, HB1175)
* strict_year: specify year for one-against-all user selection (for instance, 2006)
* inputFile: input file, one JSON per line
* outputFile: output file

Example submit command:

```bash
spark-submit --class MakeCartesian --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar
```

Example spark-shell session (Scala) to explore the output:
```bash
$ spark-shell --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar 
scala> val mydata = sc.objectFile[CartesianPair]("/user/alexeys/valid_pairs")
mydata: org.apache.spark.rdd.RDD[CartesianPair] = MapPartitionsRDD[3] at objectFile at <console>:27
scala> mydata.take(5)
```

Note the `--jars` parameter, which is intended to include various case classes defined in the code to the classpath (namely, `CartesianPair`)

## Document similarity calculation

With `DocumentAnalyzer`, one can calculate similarity among feature vectors representing the text documents. All possible combinations of eligible pairs obtained on the previous step are considered.

Following parameters need to be filled in the `resources/documentAnalyzer.conf` file:
* secThreshold: Minimum Jaccard similarity to inspect on section level 
* inputBillsFile: bill input file, one JSON per line
* inputPairsFile: `CartesianPairs` input object file
* outputMainFile: key-key pairs and corresponding similarities, as `Tuple2[Tuple2[String,String],Double]`
* outputFilteredFile: `CartesianPairs` passing similarity threshold


### Similarity calculation

### Examples

Example submit command:
```bash
spark-submit --class DocumentAnalyzer --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar
```

Example spark-shell session (Scala) to explore the output. Following example shows how to interactively load the output file having a format primary-primary key / similarity value, select only a specific version of document, sort the data by similarity values, print top 100 pairs with highest similarity to standard output:
```bash
val data = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alex/output_docs").cache()
val filtered_data = data_jaccard.filter({case ((k1,k2),v) => ((k1 contains "CO_2006_HB1175") || (k2 contains "CO_2006_HB1175"))})
val sorted_data = filtered_data.map(x => x.swap).sortByKey(false)
for (s <- sorted_data_jaccard.take(100)) {
   println(s)
}
```
More advanced interactive analysis, including plotting, is possible with `Histogrammar` package described below.

### Section-level similarity 


## Calculate document similarity: bag-of-words and TF-IDF

Calculate document/section similarity using bag-of-words and TF-IDF for feature extraction. 

Following parameters need to be filled in the `resources/adhocAnalyzer.conf` file:
* nPartitions: Number of partitions in bills_meta RDD
* numTextFeatures: Number of text features to keep in hashingTF
* measureName: Distance measure used
* inputBillsFile: bill input file, one JSON per line
* inputPairsFile: CartesianPairs object input file
* outputMainFile: key-key pairs and corresponding similarities, as `Tuple2[Tuple2[String,String],Double]`
* outputFilteredFile: `CartesianPairs` passing similarity threshold
    

Example submit command:
```bash
spark-submit  --class AdhocAnalyzer --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar
```

### Section-level similarity

## Calculate document similarity: locality sensitive hashing

## Exploratory analysis: histogramming and plotting

Considering that the `MakeCartesian` and analysis steps (for instance, `AdhocAnalyzer`) have been ran, and the object file conraining 
the primary key pairs and corresponsing similarities in the format `Tuple2[Tuple2[String,String],Double]` is available in HDFS,
one can easily perform histogram aggregation and visualization steps using Scala-based `Histogrammar` package.


### Download and install `Histogrammar`

Download and install the Histogrammar package following the isntructions here: http://histogrammar.org

### Interactive data aggragation

Start the interactive `spark-shell` session pointing to all the Histogrammar jars and the BillAnalysis jars, and do:

```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._

val data = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alex/output").cache()
val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2})
val final_histogram = data.aggregate(sim_histogram)(new Increment, new Combine)

val plot_all = final_histogram.bokeh().plot(xLabel="Jaccard",yLabel="Num. pairs")
save(plot_all,"similarities.html")
```

This will produce an html file with the plot, which you can view by pointing a webbrowser to path to that file, for instance:

```bash
firefox --no-remote file:///path_to_html_file/similarity.html
```

Read following documentation pages for more details on `histogrammar` package: http://histogrammar.org/docs/specification/
And here: http://histogrammar.org/scala/0.6/index.html#package

## Similarity of legislative proposals as a graph problem

PageRank, Dijsktra paths, GraphFrames
