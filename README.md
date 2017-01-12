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

#FIXME code smnippet for raw to JSON conversion
#FIXME code snippet for JSOn to Avro conversion

## Pre-processing and feature extraction

The feature extraction step consists of a sequence of `Spark ML` transformers intended to produce numerical feature vectors
as a dataframe column. The resulting dataframe is fed to Spark ML k-means estimator, later used to calculate the all-pairs join, and subsequently during the graph analysis step with `GraphFrames`.

### Types of features

 1. Bag-of-words and the N-gram
 1. Term frequency and inverse document frequency (TF-IDF)
 1. Minhash features

Different types of text features has been found to perform better for each type of simialrity measures. For instance, TF-IDF (small granularity N gram) +truncated SVD is best suited for cosine similarity calcualtions. Jaccard similarity perofrms best with unweighted features (i.e. MinHash or TF), larger N gram granularity is preferred for the latter.

### Dimensionality reduction

Singular value decomposition (SVD) is applied to the TF-IDF document-feature matrix to extract concepts which are most relevant for classification.

## Candidate selection and clustering  

Focusing on the document vectors which are likely to be highly similar is essential for all-pairs comparison at scale.
Modern studies employ variations of nearest-neighbor search, locality sensitive hashing, as well as sampling techniques to select a subset of rows of TF-IDF matrix based on the sparsity [DIMSUM]. 
Our approach currently utilizes k-means clustering to identify groups of documents which are likely to belong to the same diffusion topic, reducing the number of comparisons in the all-pairs similarity join calculation. In addition, `LSH` and `BucketedrandomProjectionLSH` are being added based on `Spark ML` implementation.

#FIXME copy paste the submission command for this step
#FIXME describe the configuration file parameters to show how to configure options described above


## Document similarity calculation

We consider Jaccard, Cosine, manhattan and Hamming distances. We convert those to similarities assuming inverse proportionality, and re-scale all similarities to a common range, adding an extra additive term in the denominator serves as a regularization
parameter for the case of identical vectors.

#FIXME describe the configuration file parameters to show how to configure options described above

## Exploratory analysis: histogramming and plotting

Histogrammar [http://histogrammar.org/docs/] is a suite of data aggregation primitives for making histograms, calculating descriptive statistics and plotting. A few composable functions can generate many different types of plots, and these functions are reimplemented in multiple languages and serialized to JSON for cross-platform compatibility. Histogrammar allows to aggregate data using cross-platform, functional primitives, summarizing a large dataset with discretized distributions, using lambda functions and composition rather than a restrictive set of histogram types.

To use Histogrammar in the Spark shell, you don’t have to download anything. Just start Spark with

```bash
spark-shell --packages "org.diana-hep:histogrammar_2.11:1.0.4"
```
and call

```scala
import org.dianahep.histogrammar._
```
on the Spark prompt. For plotting with Bokeh, `include org.diana-hep:histogrammar-bokeh_2.11:1.0.4` and for interaction with Spark-SQL, include `org.diana-hep:histogrammar-sparksql_2.11:1.0.4`.

### Example of stat analysis with Spark and Histogrammar

Given the cosine and jaccard output files on the key-key pair, and convert it to dataframe:

```scala
val data = cosineRDD.join(jaccardRDD).toDF("cosine","jaccard")
data.write.parquet("/user/alexeys/correlations_3state")
```

Launch spark-shell session with histogrammar pre-loaded:

```bash
spark-shell --master yarn --queue production --num-executors 20 --executor-cores 3 --executor-memory 10g --packages "org.diana-hep:histogrammar-bokeh_2.10:1.0.3" --jars target/scala-2.11/BillAnalysis-assembly-2.0.jar 
```

Get basic descriptive statistics:

```scala
scala> data.describe().show()
+-------+--------------------+------------------+
|summary|              cosine|           jaccard|
+-------+--------------------+------------------+
|  count|          2632811191|        2632811191|
|   mean|   2.648025009784054|11.899197957421478|
| stddev|  3.2252594746900303|3.5343401388251032|
|    min|2.389704494502045...|1.4545454545454546|
|    max|   98.63368807585896| 81.14406779661016|
+-------+--------------------+------------------+
```

Get correlation coefficients and distributions in 10 bins:

```scala
scala> val cosine_rdd = data.select("cosine").rdd.map(x=>x.getDouble(0))
cosine_rdd: org.apache.spark.rdd.RDD[Double] = MapPartitionsRDD[4] at map at <console>:27

scala> val jaccard_rdd = data.select("jaccard").rdd.map(x=>x.getDouble(0))
jaccard_rdd: org.apache.spark.rdd.RDD[Double] = MapPartitionsRDD[7] at map at <console>:27

scala> import org.dianahep.histogrammar._
import org.dianahep.histogrammar._

scala> import org.dianahep.histogrammar.ascii._
import org.dianahep.histogrammar.ascii._

scala> val histo = Histogram(10,0,100,{x: Double => x})
histo: org.dianahep.histogrammar.Selecting[Double,org.dianahep.histogrammar.Binning[Double,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]] = <Selecting cut=Bin>

scala> val jaccard_histo = jaccard_rdd.aggregate(histo)(new Increment, new Combine)
jaccard_histo: org.dianahep.histogrammar.Selecting[Double,org.dianahep.histogrammar.Binning[Double,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]] = <Selecting cut=Bin>

scala> jaccard_histo.println
                       +----------------------------------------------------------+
underflow     0        |                                                          |
[  0 ,  10 )  7.943E+8 |***********************                                   |
[  10,  20 )  1.792E+9 |*****************************************************     |
[  20,  30 )  4.668E+7 |*                                                         |
[  30,  40 )  2.269E+5 |                                                          |
[  40,  50 )  5661     |                                                          |
[  50,  60 )  572      |                                                          |
[  60,  70 )  125      |                                                          |
[  70,  80 )  57       |                                                          |
[  80,  90 )  4        |                                                          |
[  90,  100)  0        |                                                          |
overflow      0        |                                                          |
nanflow       0        |                                                          |
                       +----------------------------------------------------------+
```                      

For more details on how to use Histogrammar, refer to the website: http://histogrammar.org/docs/

## Reformulating the problem as a network (graph) problem

Some policy diffusion questions are easier answered if the problem is formulated as a graph analysis problem. The dataframe output of the document similarity step is mapped onto a weighted undirected graph, considering each unique legislative proposal as a node and a presence of a document with similarity above a certain threshold as an edge with a weight attribute equal to the similarity. 

The PageRank and Dijkstra minimum cost path algorithms are applied to detect events of policy diffusion and the most influential states. A GraphFrame is constructed using two dataframes (a dataframe of nodes and an edge dataframe), allowing to easily integrate the graph processing step into the pipeline along with Spark ML, without a need to move the results of previous steps manually and feeding them to the graph processing module from an intermediate sink, like with isolated graph analysis systems.

#FIXME describe the configuration file parameters to show how to configure options described above
