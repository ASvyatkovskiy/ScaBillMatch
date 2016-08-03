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
