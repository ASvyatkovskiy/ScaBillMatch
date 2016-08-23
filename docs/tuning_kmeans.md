# How to tune k-means

The number of classes in k-means clustering procedure is a free parameter. An initial estimate for k can be obtained from the 
"subject" line in the LexisNexis dataset, by simply tokenizing the subject and taking distnict values.

The typical parameter search procedure would involve repeating the clusting procedure for a given number of iterations
on a grid of values of target classes (of the order of the number obtained in the previous step).

We are looking for a trade-off between the processing time and the accuracy, and an elbow structure in the loss vs k plot 
(if the processing time is not an issue).

To better assess the number of classes in the k-means clustering, we can explore the Parquet output file,
group data by class label and aggregate it to count how many samples end up in a given class:

```scala
val data = sqlContext.read.parquet("/user/alexeys/kMeansLabels_FL_MI_SC").cache()
val groupedData = data.groupBy("prediction")

import sqlContext.implicits._
import org.apache.spark.sql.functions._
val counted = groupedData.agg('prediction, countDistinct('primary_key))
```

To validate the accuracy, we need to also make sure that all bills tagged as stand your ground bills by us, 
all end up in the same class:

```scala
val data = sqlContext.read.parquet("/user/alexeys/kMeansLabels_20").cache()
val filtered_data = data.filter($"primary_key" === "FL_2005_SB436_Introduced") 
```

And test that they all belong to the same class:

```scala
scala> val data = sqlContext.read.parquet("/user/alexeys/kMeansLabels_20").cache()
data: org.apache.spark.sql.DataFrame = [primary_key: string, prediction: int]

scala> 

scala> val filtered_data = data.filter($"primary_key" === "FL_2005_SB436_Introduced")
filtered_data: org.apache.spark.sql.DataFrame = [primary_key: string, prediction: int]

scala> filtered_data.show(false)
+------------------------+----------+
|primary_key             |prediction|
+------------------------+----------+
|FL_2005_SB436_Introduced|62        |
+------------------------+----------+


scala> val filtered_data = data.filter($"primary_key" === "SC_2005_HB4301_Introduced")
filtered_data: org.apache.spark.sql.DataFrame = [primary_key: string, prediction: int]

scala> filtered_data.show(false)
+-------------------------+----------+
|primary_key              |prediction|
+-------------------------+----------+
|SC_2005_HB4301_Introduced|62        |
+-------------------------+----------+


scala> val filtered_data = data.filter($"primary_key" === "MI_2005_SB1046_Introduced")
filtered_data: org.apache.spark.sql.DataFrame = [primary_key: string, prediction: int]

scala> filtered_data.show(false)
+-------------------------+----------+
|primary_key              |prediction|
+-------------------------+----------+
|MI_2005_SB1046_Introduced|62        |
+-------------------------+----------+


scala> val filtered_data = data.filter($"primary_key" === "MI_2005_HB5153_Introduced")
filtered_data: org.apache.spark.sql.DataFrame = [primary_key: string, prediction: int]

scala> filtered_data.show(false)
+-------------------------+----------+
|primary_key              |prediction|
+-------------------------+----------+
|MI_2005_HB5153_Introduced|62        |
+-------------------------+----------+
```

Note: test with 20 states showed the optimal number lays between 150 and 200 classes.

# Note on number of permutations

The point of running k-means clustering on the pre-processing step is to determine candidate pairs, and olny calculate distances between candidates.

For the 3 state example, considering Introduced docversion only, we have 212768 pairs, resulting in 

```scala
scala> N*(N-1)/2.0
res7: Double = 2.2635004528E10
```

distinct pairs. Considering 150 classes, and assuming uniform distribution of samples among these clusters we get:

```scala
scala> 212768.0/150.0
res8: Double = 1418.4533333333334

scala> 1418*(1418-1)/2.0*150.0
res10: Double = 1.5069795E8
```

roughly 2 orders of magnitude less pairs.
The actual distribution among k-means clusters is far from being uniform:

```scala
scala> val data = spark.read.parquet("/user/alexeys/bills_combined_3").repartition(10)
data: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [primary_key: string, docversion: string ... 5 more fields]

scala> val groupedData = data.groupBy("prediction")
groupedData: org.apache.spark.sql.RelationalGroupedDataset = org.apache.spark.sql.RelationalGroupedDataset@26f0ab88

scala> val counted = groupedData.agg('prediction, countDistinct('primary_key).as("counts_per_cluster"))
counted: org.apache.spark.sql.DataFrame = [prediction: int, prediction: int ... 1 more field]

scala> counted.show(200)
+----------+----------+---------------------------+
|prediction|prediction|counts_per_cluster         |
+----------+----------+---------------------------+
|       148|       148|                         45|
|        31|        31|                          6|
|       137|       137|                         12|
|        85|        85|                          1|
|        65|        65|                         73|
|        53|        53|                      20528|
|       133|       133|                      27800|
|        78|        78|                       4182|
|        34|        34|                          2|
|       126|       126|                         99|
|       101|       101|                         32|
|       115|       115|                          7|
|        81|        81|                          7|
|        28|        28|                         73|
|        76|        76|                        665|
|        27|        27|                         39|
|        26|        26|                        200|
|        44|        44|                         17|
|       103|       103|                          4|
|        12|        12|                          5|
|        91|        91|                        678|
|        22|        22|                          2|
|       122|       122|                          5|
|        93|        93|                          2|
|       111|       111|                        384|
|        47|        47|                          1|
|       140|       140|                         15|
|       132|       132|                       1880|
|       146|       146|                        181|
|         1|         1|                         30|
|        52|        52|                         93|
|        13|        13|                          2|
|        86|        86|                         41|
|         6|         6|                          7|
|        16|        16|                          1|
|         3|         3|                       3451|
...
```

Resulting in:

```scala
scala> def npermutations = udf((s: Integer) => s*(s-1)/2.0)
npermutations: org.apache.spark.sql.expressions.UserDefinedFunction

scala> val f = counted.withColumn("npermutats",npermutations(col("counts_per_cluster")))
f: org.apache.spark.sql.DataFrame = [prediction: int, prediction: int ... 2 more fields]

scala> f.show()
+----------+----------+------------------+------------+                         
|prediction|prediction|counts_per_cluster|  npermutats|
+----------+----------+------------------+------------+
|       148|       148|                45|       990.0|
|        31|        31|                 6|        15.0|
|       137|       137|                12|        66.0|
|        85|        85|                 1|         0.0|
|        65|        65|                73|      2628.0|
|        53|        53|             20528|2.10689128E8|
|       133|       133|             27800|  3.864061E8|
|        78|        78|              4182|   8742471.0|
|        34|        34|                 2|         1.0|
|       126|       126|                99|      4851.0|
|       101|       101|                32|       496.0|
|       115|       115|                 7|        21.0|
|        81|        81|                 7|        21.0|
|        28|        28|                73|      2628.0|
|        76|        76|               665|    220780.0|
|        27|        27|                39|       741.0|
|        26|        26|               200|     19900.0|
|        44|        44|                17|       136.0|
|       103|       103|                 4|         6.0|
|        12|        12|                 5|        10.0|
+----------+----------+------------------+------------+
only showing top 20 rows


scala> val f = counted.withColumn("npermutats",npermutations(col("counts_per_cluster"))).agg(sum(col("npermutats")))
f: org.apache.spark.sql.DataFrame = [sum(npermutats): double]

scala> f.show()
+---------------+                                                               
|sum(npermutats)|
+---------------+
|   8.38707813E8|
+---------------+
```

resulting in a close (acceptable) number - same oder of magnitude, 4 times larger.

