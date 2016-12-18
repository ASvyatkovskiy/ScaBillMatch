import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.clustering.{KMeans, BisectingKMeans}

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

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.linalg.{
   Vector => NewVector,
   Vectors => NewVectors,
   DenseVector => NewDenseVector,
   SparseVector => NewSparseVector
}

import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object AnalysisUtils {

  def sampleNOrdered(spark: SparkSession, raw_input_filename: String, processed_input_filename: String, numRows: Int, isAscending: Boolean) : DataFrame = {  
    import spark.implicits._

    var raw_bills = spark.read.json(raw_input_filename).filter(col("docversion") === "Introduced")
    raw_bills = raw_bills.withColumn("content",Utils.cleaner_udf(col("content")))
    val processed_data = spark.sparkContext.objectFile[Tuple2[Tuple2[String,String],Double]](processed_input_filename).map(x=>(x._1._1,x._1._2,x._2)).toDF("pk1","pk2","similarity").cache()
    var sorted = processed_data.sort(desc("similarity")).limit(numRows)
    if (isAscending) {
       sorted = processed_data.sort(asc("similarity")).limit(numRows)
    }
    val j1 = sorted.join(raw_bills.withColumnRenamed("content","content1"),$"pk1" === $"primary_key").select("pk1","pk2","content1","similarity")
    val j2 = j1.join(raw_bills.withColumnRenamed("content","content2"),$"pk2" === $"primary_key").select("pk1","pk2","content1","content2","similarity") //.write.parquet("/user/path/to/output")
    //to sort by similarity do
    if (isAscending) {
      j2.sort(asc("similarity"))
    } else {
      j2.sort(desc("similarity"))
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
}
