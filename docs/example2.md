# Jaccard vs cosine

```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._

val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2})

val data_jaccard = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_jaccard_nFeat100k").cache()
val filtered_data_jaccard = data_jaccard.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).cache()
val final_histogram = filtered_data_jaccard.aggregate(sim_histogram)(new Increment, new Combine)
val plot_jaccard_all = final_histogram.bokeh()

val data_cosine = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_cosine_nFeat100k").cache()
val filtered_data_cosine = data_cosine.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).cache()
val final_histogram2 = filtered_data_cosine.aggregate(sim_histogram)(new Increment, new Combine)

import io.continuum.bokeh._
val plot_cosine_all = final_histogram2.bokeh(lineColor=Color.Blue)

val my = plot(xLabel="Similarity",yLabel="Num. pairs",plot_jaccard_all,plot_cosine_all)

val legend = List("Jaccard" -> List(plot_jaccard_all),"Cosine" -> List(plot_cosine_all))
val leg = new Legend().plot(my).legends(legend)
my.renderers <<= (leg :: _)

save(my,"cosine_jaccard_filtered.html")
```

# Jaccard vs cosine zoomed to tail
```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._

val sim_histogram = Histogram(200, 30, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2})

val data_jaccard = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_jaccard_nFeat100k").cache()
val filtered_data_jaccard = data_jaccard.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).cache()
val final_histogram = filtered_data_jaccard.aggregate(sim_histogram)(new Increment, new Combine)
val plot_jaccard_all = final_histogram.bokeh()

val data_cosine = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_cosine_nFeat100k").cache()
val filtered_data_cosine = data_cosine.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).cache()
val final_histogram2 = filtered_data_cosine.aggregate(sim_histogram)(new Increment, new Combine)

import io.continuum.bokeh._
val plot_cosine_all = final_histogram2.bokeh(lineColor=Color.Blue)

val my = plot(xLabel="Similarity",yLabel="Num. pairs",plot_jaccard_all,plot_cosine_all)

val legend = List("Jaccard" -> List(plot_jaccard_all),"Cosine" -> List(plot_cosine_all))
val leg = new Legend().plot(my).legends(legend)
my.renderers <<= (leg :: _)

save(my,"cosine_jaccard_filtered.html")

```

# Print top similarity matches for this selections

Note, that when we are only interested in top similarity matches, we do not need to carry the bulk of the distribution around. So we apply a filter before sorting step which saves hours of work:
```scala
....filter({case ((k1,k2),v) => (v > 70.0)})
```

Here is the complete workflow:

```scala
val data_jaccard = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_jaccard_nFeat100k").cache()
val filtered_data_jaccard = data_jaccard.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).filter({case ((k1,k2),v) => (v > 70.0)}).cache()

val sorted_data_jaccard = filtered_data_jaccard.map(x => x.swap).sortByKey(false)
for (s <- sorted_data_jaccard.take(100)) {
    println(s)
}

val data_cosine = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_cosine_nFeat100k").cache()
val filtered_data_cosine = data_cosine.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))}).cache()

val sorted_data_cosine = filtered_data_cosine.map(x => x.swap).sortByKey(false)
for (s <- sorted_data_cosine.take(100)) {
    println(s)
}

```
