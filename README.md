# Prepare a histogram of similarities

Considering that the MakeCartesian and analysis steps (for instance, AdhocAnalyzer) have been ran, and the object file conraining 
the primary key pairs and corresponsing similarities in the format `Tuple2[Tuple2[Long,Long],Double]` is available in HDFS,
one can easily perform histogram aggregation and visualization steps using Scala-based `Histogrammar` package.


## Download and install `Histogrammar`

Download and install the Histogrammar package following the isntructions here: http://histogrammar.org


## Interactive data aggragation and plotting

Start the interactive `spark-shell` session pointing to all the Histogrammar jars and the BillAnalysis jars, and do:

```scala
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.1
      /_/

Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_51)
Type in expressions to have them evaluated.
Type :help for more information.
Spark context available as sc.
SQL context available as sqlContext.

scala> import org.dianahep.histogrammar._
import org.dianahep.histogrammar._

scala> import org.dianahep.histogrammar.bokeh._
import org.dianahep.histogrammar.bokeh._

scala> import java.io._
import java.io._

scala> val data = sc.objectFile[Tuple2[Tuple2[Long,Long],Double]]("/user/alexeys/test_main_output").cache()
data: org.apache.spark.rdd.RDD[((Long, Long), Double)] = MapPartitionsRDD[1] at objectFile at <console>:36

scala> val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[Long,Long],Double] => matches._2})
sim_histogram: org.dianahep.histogrammar.Selecting[((Long, Long), Double),org.dianahep.histogrammar.Binning[((Long, Long), Double),org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]] = Selecting[Binning[low=0.0, high=100.0, values=[Counting[0.0]..., size=200], underflow=Counting[0.0], overflow=Counting[0.0], nanflow=Counting[0.0]]]

scala> val all_histograms = Label("Similarity" -> sim_histogram)
all_histograms: org.dianahep.histogrammar.Labeling[org.dianahep.histogrammar.Selecting[((Long, Long), Double),org.dianahep.histogrammar.Binning[((Long, Long), Double),org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]]] = Labeling[[(Similarity,Selecting[Binning[low=0.0, high=100.0, values=[Counting[0.0]..., size=200], underflow=Counting[0.0], overflow=Counting[0.0], nanflow=Counting[0.0]]])..., size=1]]

scala> val final_histogram = data.aggregate(all_histograms)(new Increment, new Combine)
final_histogram: org.dianahep.histogrammar.Labeling[org.dianahep.histogrammar.Selecting[((Long, Long), Double),org.dianahep.histogrammar.Binning[((Long, Long), Double),org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]]] = Labeling[[(Similarity,Selecting[Binning[low=0.0, high=100.0, values=[Counting[0.0]..., size=200], underflow=Counting[0.0], overflow=Counting[4.04917543E8], nanflow=Counting[0.0]]])..., size=1]]

scala> val my = final_histogram("Similarity").bokeh().plot()
my: io.continuum.bokeh.Plot = io.continuum.bokeh.Plot@3e83ab7b

scala> save(my,"similarity.html")
Wrote similarity.html. Open file:///home/alexeys/HEPSparkTests/histogrammar/scala-bokeh/similarity.html in a web browser.
```
