```scala
import org.dianahep.histogrammar._
import org.dianahep.histogrammar.bokeh._

val sim_histogram = Histogram(200, 0, 100, {matches: Tuple2[Tuple2[String,String],Double] => matches._2})

val data_jaccard = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_jaccard_MLFLtest_Introduced_diffstate_grouped").cache()
val data_cosine = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_cosine_MLFLtest_Introduced_diffstate_grouped").cache()
val data_manhattan = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_manhattan_MLFLtest_Introduced_diffstate_grouped").cache()
val data_hamming = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_hamming_MLFLtest_Introduced_diffstate_grouped").cache()
val final_jaccard = data_jaccard.aggregate(sim_histogram)(new Increment, new Combine)
val final_cosine = data_cosine.aggregate(sim_histogram)(new Increment, new Combine)
val final_manhattan = data_manhattan.aggregate(sim_histogram)(new Increment, new Combine)
val final_hamming = data_hamming.aggregate(sim_histogram)(new Increment, new Combine)

import io.continuum.bokeh._
val plot_jaccard_all = final_jaccard.bokeh()
val plot_cosine_all = final_cosine.bokeh(lineColor=Color.Blue)
val plot_manhattan_all = final_manhattan.bokeh(lineColor=Color.Red)
val plot_hamming_all = final_hamming.bokeh(lineColor=Color.Green)
val my = plot(xLabel="Similarity",yLabel="Num. pairs",plot_jaccard_all,plot_cosine_all,plot_manhattan_all,plot_hamming_all)
val legend = List("Jaccard" -> List(plot_jaccard_all),"Cosine" -> List(plot_cosine_all), "Manhattan" -> List(plot_manhattan_all), "Hamming" -> List(plot_hamming_all))
val leg = new Legend().plot(my).legends(legend)
my.renderers <<= (leg :: _)
save(my,"all_simis.html")
```
