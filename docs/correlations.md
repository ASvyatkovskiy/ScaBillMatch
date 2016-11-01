# Prepararions

Join the cosine and jaccard output files on the key-key pair, and convert it to dataframe:

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
                       0                                                 1.97076e+09
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

scala> val cosine_histo = cosine_rdd.aggregate(histo)(new Increment, new Combine)
cosine_histo: org.dianahep.histogrammar.Selecting[Double,org.dianahep.histogrammar.Binning[Double,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting,org.dianahep.histogrammar.Counting]] = <Selecting cut=Bin>

scala> cosine_histo.println
                       0                                                 2.80696e+09
                       +----------------------------------------------------------+
underflow     0        |                                                          |
[  0 ,  10 )  2.552E+9 |*****************************************************     |
[  10,  20 )  6.682E+7 |*                                                         |
[  20,  30 )  1.045E+7 |                                                          |
[  30,  40 )  2.654E+6 |                                                          |
[  40,  50 )  7.953E+5 |                                                          |
[  50,  60 )  2.213E+5 |                                                          |
[  60,  70 )  6.631E+4 |                                                          |
[  70,  80 )  1.759E+4 |                                                          |
[  80,  90 )  6021     |                                                          |
[  90,  100)  444      |                                                          |
overflow      0        |                                                          |
nanflow       0        |                                                          |
                       +----------------------------------------------------------+
```

Finally, I calculate the Pearson correlation coefficient:

```scala
scala> import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.Statistics

scala> val correlation: Double = Statistics.corr(jaccard_rdd, cosine_rdd, "pearson")
correlation: Double = 0.32798598370214804     
```
