package org.princeton.billmatch

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import java.io._

object LatestVersionExtracter {

  val monthNameToNumber = Map(
    "January"   -> "01",
    "February"  -> "02",
    "March"     -> "03",
    "April"     -> "04",
    "May"       -> "05",
    "June"      -> "06",
    "July"      -> "07",
    "August"    -> "08",
    "September" -> "09",
    "October"   -> "10",
    "November"  -> "11",
    "December"  -> "12"
  )

  def zeroPrefixed(s: String) : String = {
     val l = s.length
     val result = l match {
        case 1 => "0"+s
        case other => s
     }
     result
  }

  def rtrim(s: String) = s.replaceAll(",$", "")

  def getTimestampString = udf((s: String) => Array(monthNameToNumber(s.split(" ")(0)),zeroPrefixed(rtrim(s.split(" ")(1))),s.split(" ")(2)).mkString("-"))

  def getPK = udf((filePath: String, version: String) => Array(filePath.split("/")(1),filePath.split("/")(2),filePath.split("/")(3)).mkString("_"))
  def customPK = udf((primary_key: String) => primary_key.split("_").slice(0,3).mkString("_"))
  def getTimestamp = to_timestamp(col("timestamp_string"), "MM-dd-yyyy")

  def getLatest(sequence: Seq[String]) : String = {
   if (sequence.length == 1) {return "Introduced" }
   else if (sequence.contains("Enacted")) { return "Enacted" }
   else if (sequence.contains("Enrolled")) {return "Enrolled"}
   else if (sequence.contains("Adopted")) {return "Adopted"}
   else if (sequence.contains("Substituted")) {return "Substituted"}
   else if (sequence.contains("Amended")) {return "Amended"}
   else if (sequence.contains("Reintroduced")) {return "Reintroduced"}
   else {return sequence(0)}
  }

  class SeqAgg() extends Aggregator[(String,String), Seq[String], Seq[String]] {
     def zero: Seq[String] = Nil //this should have a type of merge buffer
     def reduce(b: Seq[String], a: (String,String)): Seq[String] = a._2 +: b
     def merge(b1: Seq[String], b2: Seq[String]): Seq[String] = b1 ++ b2
     def finish(reduction: Seq[String]): Seq[String] = reduction
     override def bufferEncoder: Encoder[Seq[String]] = ExpressionEncoder()
     override def outputEncoder: Encoder[Seq[String]] = ExpressionEncoder() 
  }

  def main (args: Array[String]) {

    val t0 = System.nanoTime()
    val spark = SparkSession
      .builder()
      .appName("LatestVersionExtracter")
      .config("spark.shuffle.service.enabled","true")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.json("/user/alexeys/metadata/metNJ.json").select("filePath","versionDate","version").as[Metadata]
    val data_w_timestamps = data.withColumn("timestamp_string",getTimestampString(col("versionDate"))).withColumn("timestamp", getTimestamp) //drop("")

    val ready_to_join = data_w_timestamps.withColumn("primary_key_to_remove",getPK($"filePath",$"version")).select("primary_key_to_remove","version").as[(String,String)]

    val myAgg = new SeqAgg()
    val results = ready_to_join.groupByKey(_._1).agg(myAgg.toColumn).map{case (x,y) => (x,getLatest(y))}

    results.printSchema()
    results.show(40,false)

    //for (d <- results.take(10)) {
    //    println(d)
    //}
    //results.write.parquet("/user/alexeys/new_3_state_test") 
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }
} 
case class Metadata(filePath: String,versionDate: String, version: String)
