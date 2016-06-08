# ScaBillMatch [![Build Status](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)

Scala based reboot of diffusion study (bill match)

## Calculate candidate pairs 

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
res1: Array[CartesianPair] = Array()
```

Note the `--jars` parameter, which is intended to include various case classes defined in the code to the classpath (namely, `CartesianPair`)

## Calculate document similarity (workflow 1)

With `DocumentAnalyzer`, one can calculate similarity among feature vectors representing the text documents. All possible combinations of eligible pairs obtained on the previous step are considered.

FIXME expand with details on the method

Following parameters need to be filled in the `resources/documentAnalyzer.conf` file:
* secThreshold: Minimum Jaccard similarity to inspect on section level 
* inputBillsFile: bill input file, one JSON per line
* inputPairsFile: `CartesianPairs` input object file
* outputMainFile: key-key pairs and corresponding similarities, as `Tuple2[Tuple2[String,String],Double]`
* outputFilteredFile: `CartesianPairs` passing similarity threshold

Example submit command:
```bash
spark-submit --class DocumentAnalyzer --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar
```

Example spark-shell session (Scala) to explore the output:
```bash
```

Following example shows how to interactively load the output file having a format primary-primary key / similarity value, select only a specific version of document, sort the data by similarity values, print it to the screen:

```scala
val data = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output").cache()
val filtered = data.filter({case ((k1,k2),v) => ((k1 contains "CO_2006_HB1175") || (k1 contains "CO_2006_HB1175"))}).cache()
val sorted = filtered.map(x => x.swap).sortByKey(false).cache()
sorted.foreach(println)
```

More advanced interactive analysis, including plotting, is possible with `Histogrammar` package described below.

## Calculate document similarity (workflow 2)

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

val data = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/test_main_output").cache()
val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2})
val final_histogram = data.aggregate(sim_histogram)(new Increment, new Combine)

val my = final_histogram.bokeh().plot()
save(my,"similarity.html")
```

This will produce an html file with the plot, which you can view by pointing a webbrowser to path to that file, for instance:

```bash
firefox --no-remote file:///home/alexeys/similarity.html
```

Read following documentation pages for more details on `histogrammar` package: http://histogrammar.org/docs/specification/
And here: http://histogrammar.org/scala/0.6/index.html#package
