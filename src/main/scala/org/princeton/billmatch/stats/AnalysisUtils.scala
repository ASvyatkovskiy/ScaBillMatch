package org.princeton.billmatch
package stats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.clustering.{KMeans, BisectingKMeans}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,CoordinateMatrix}

//we have to deal with this nonsense for now
import org.apache.spark.mllib.linalg.{
  Vector => OldVector,
  Vectors => OldVectors,
  SparseVector => OldSparseVector,
  DenseVector => OldDenseVector,
  VectorUDT => OldVectorUDT}

import org.apache.spark.ml.linalg.{
   Vector => NewVector,
   Vectors => NewVectors,
   DenseVector => NewDenseVector,
   SparseVector => NewSparseVector
}

import org.apache.spark.rdd.RDD

import org.princeton.billmatch.feature._

object AnalysisUtils {

  def sampleNOrdered(spark: SparkSession, raw_input_filename: String, processed_input_filename: String, numRows: Int, isAscending: Boolean) : DataFrame = {  
    import spark.implicits._

    val input = spark.read.json(raw_input_filename).filter(col("docversion") === "Introduced")
    val npartitions = (input.count()/1000).toInt

    val raw_bills = input.withColumn("content",Utils.cleaner_udf(col("content"))).cache()

    val processed_data = spark.read.parquet(processed_input_filename)
    val sorted = if (isAscending) processed_data.sort(asc("similarity")).limit(numRows) else processed_data.sort(desc("similarity")).limit(numRows)
    val sorted_dataset = sorted.as[ComparedPair].repartition(npartitions).cache()

    val j1 = sorted_dataset.join(raw_bills.withColumnRenamed("content","content1"),$"pk1" === $"primary_key").select("pk1","pk2","content1","similarity")
    val j2 = j1.join(raw_bills.withColumnRenamed("content","content2"),$"pk2" === $"primary_key").select("pk1","pk2","content1","content2","similarity") //.write.parquet("/user/path/to/output")
    //to sort by similarity do
    if (isAscending) {
      j2.sort(asc("similarity"))
    } else {
      j2.sort(desc("similarity"))
    }
  } 

  def sampleNRandom(spark: SparkSession, raw_input_filename: String, processed_input_filename: String, numRows: Int, isAscending: Boolean, threshold: Double) : DataFrame = {
    import spark.implicits._

    val input = spark.read.json(raw_input_filename).filter(col("docversion") === "Introduced")
    val npartitions = (input.count()/1000).toInt

    val raw_bills = input.withColumn("content",Utils.cleaner_udf(col("content"))).cache()

    val processed_data = spark.read.parquet(processed_input_filename)
    
    val sorted = if(isAscending) processed_data.filter(processed_data("similarity") <= threshold) else processed_data.filter(processed_data("similarity") >= threshold)
    val sorted_dataset = sorted.as[ComparedPair].repartition(npartitions).cache()
    
    val j1 = sorted_dataset.join(raw_bills.withColumnRenamed("content","content1"),$"pk1" === $"primary_key").select("pk1","pk2","content1","similarity")
    val j2 = j1.join(raw_bills.withColumnRenamed("content","content2"),$"pk2" === $"primary_key").select("pk1","pk2","content1","content2","similarity")

    var fraction: Double = numRows.toDouble/sorted.count()
    if (fraction < 1.0) {
      if (isAscending) {
        j2.sample(false,fraction).sort(asc("similarity"))
      } else {
        j2.sample(false,fraction).sort(desc("similarity"))
      }
    } else {
      println("The dataset fraction exceeds 1.0, returning the whole dataset above similarity threshold") 
      if (isAscending) {
        j2.sort(asc("similarity"))
      } else {
        j2.sort(desc("similarity"))
      }      
    }
  }

  def cleanSubsample(spark: SparkSession, df: DataFrame) : DataFrame = {

    val cleaned_df = df.withColumn("cleaned",Utils.cleaner_udf(col("content"))).drop("content")
    val tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("content")
    val prefeaturized_df = remover.transform(tokenized_df).drop("words")
    prefeaturized_df

  }

  def takeLargerPk_udf = udf((pk1: String, pk2: String) => {
        val state1 = pk1.split("_")(1)
        val state2 = pk2.split("_")(1)

        if (state1 > state2) pk1
        else pk2
    })

  def takeSmallerPk_udf = udf((pk1: String, pk2: String) => {
        val state1 = pk1.split("_")(1)
        val state2 = pk2.split("_")(1)

        if (state1 > state2) pk2
        else pk1
    })

  def takeSmallerContent_udf = udf((pk1: String, pk2: String,content1: String, content2: String) => {
        val state1 = pk1.split("_")(1)
        val state2 = pk2.split("_")(1)

        if (state1 > state2) content2
        else content1
    })

  def takeLargerContent_udf = udf((pk1: String, pk2: String,content1: String, content2: String) => {
        val state1 = pk1.split("_")(1)
        val state2 = pk2.split("_")(1)

        if (state1 > state2) content1
        else content2
    })


  def imposeTemporalOrder(df: DataFrame) : DataFrame = {
     val columns = df.schema.fields.map(x=>x.name)
     if columns contains "content" {
         df.withColumn("content1_smaller",takeSmallerContent_udf(col("pk1"),col("pk2"),col("content1"),col("content2"))).withColumn("content2_larger",takeLargerContent_udf(col("pk1"),col("pk2"),col("content1"),col("content2"))).withColumn("pk1_smaller",takeSmallerPk_udf(col("pk1"),col("pk2"))).withColumn("pk2_larger",takeLargerPk_udf(col("pk1"),col("pk2"))).drop(col("pk1")).drop(col("pk2")).drop(col("content1")).drop(col("content2"))
     } else {
         df.withColumn("pk1_smaller",takeSmallerPk_udf(col("pk1"),col("pk2"))).withColumn("pk2_larger",takeLargerPk_udf(col("pk1"),col("pk2"))).drop(col("pk1")).drop(col("pk2"))
     }
     
  }

}
case class ComparedPair(pk1: String, pk2: String, similarity: Double)
