#1 histogram no filter no zoom

```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._

val data_jaccard = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_10000_jaccard_nFeat100k").cache()
val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2}) 
val final_histogram = data_jaccard.aggregate(sim_histogram)(new Increment, new Combine)
val plot_jaccard_all = final_histogram.bokeh().plot(xLabel="Jaccard",yLabel="Num. pairs")
save(plot_jaccard_all,"adhoc_jaccard_all.html")

val filtered_data = data_jaccard.filter({case ((k1,k2),v) => ((k1 contains "NJ") || (k2 contains "NJ"))})
val sorted_data = filtered_data.map(x => x.swap).sortByKey(false)
for (s <- sorted_data.take(100)) {
    println(s)
}
```
