# ScaBillMatch [![Build Status](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)

Scala based reboot of diffusion study (bill match)

## Calculate eligible candidate pairs 

`MakeCartesian`, will produce all the pairs of primary keys of the documents satisfying a predicate.
Following parameters need to be filled in the `resources/makeCartesian.conf` file:
* docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted...
* nPartitions: number of partitions in bills_meta RDD
* use_strict: boolean, yes or no to consider strict parameters
* strict_state: specify state (a long integer from 1 to 50) for one-against-all user selection (strict)
* strict_docid: specify document ID for one-against-all user selection (strict)
* strict_year: specify year for one-against-all user selection (strict)
* inputFile: input file, one JSON per line
* outputFile: output file

Example to explore output in spark-shell:
```bash
$ spark-shell --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar 
scala> val mydata = sc.objectFile[CartesianPair]("/user/alexeys/valid_pairs")
mydata: org.apache.spark.rdd.RDD[CartesianPair] = MapPartitionsRDD[3] at objectFile at <console>:27
scala> mydata.take(5)
res1: Array[CartesianPair] = Array()
```

## Calculate document similarity (workflow 1)

With `DocumentAnalyzer`, one can calculate all-pairs similarity considering possible combinations of eligible pairs obtained on the previous step.

Following parameters need to be filled in the `resources/documentAnalyzer.conf` file:
* secThreshold: Minimum Jaccard similarity to inspect on section level 
* inputBillsFile: Bill input file, one JSON per line
* inputPairsFile: CartesianPairs object input file
* outputMainFile: outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[Long,Long],Double]
* outputFilteredFile: CartesianPairs passing similarity threshold

## Calculate document similarity (workflow 2)

Calculate document/section similarity using bag-of-words and TF-IDF. Spark ML library and Dataframes are used.

Following parameters need to be filled in the `resources/adhocAnalyzer.conf` file:
* nPartitions: Number of partitions in bills_meta RDD
* numTextFeatures: Number of text features to keep in hashingTF
* measureName: Distance measure used
* inputBillsFile: Bill input file, one JSON per line
* inputPairsFile: CartesianPairs object input file
* outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[Long,Long],Double]
* outputFilteredFile: CartesianPairs passing similarity threshold
    
## Prepare a histogram of similarities

Considering that the MakeCartesian and analysis steps (for instance, AdhocAnalyzer) have been ran, and the object file conraining 
the primary key pairs and corresponsing similarities in the format `Tuple2[Tuple2[Long,Long],Double]` is available in HDFS,
one can easily perform histogram aggregation and visualization steps using Scala-based `Histogrammar` package.


### Download and install `Histogrammar`

Download and install the Histogrammar package following the isntructions here: http://histogrammar.org


### Interactive data aggragation and plotting

Start the interactive `spark-shell` session pointing to all the Histogrammar jars and the BillAnalysis jars, and do:

```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._
import java.io._

val data = sc.objectFile[Tuple2[Tuple2[Long,Long],Double]]("/user/alexeys/test_main_output").cache()
val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[Long,Long],Double] => matches._2})
val all_histograms = Label("Similarity" -> sim_histogram)
val final_histogram = data.aggregate(all_histograms)(new Increment, new Combine)

val my = final_histogram("Similarity").bokeh().plot()
save(my,"similarity.html")
```

This will produce an html file with the plot, which you can view by pointing a webbrowser to path to that file, for instance:

```bash
firefox --no-remote file:///home/alexeys/similarity.html
```
