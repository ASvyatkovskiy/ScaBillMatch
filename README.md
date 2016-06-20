# ScaBillMatch [![Build Status](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)](https://travis-ci.org/ASvyatkovskiy/ScaBillMatch.svg?branch=master)

Scala based reboot of diffusion study (bill match)

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
