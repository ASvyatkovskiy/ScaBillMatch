package org.princeton.billmatch

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

  def getTimestampString(s: String) : String = {
    try { 
      Array(monthNameToNumber(s.split(" ")(0)),zeroPrefixed(rtrim(s.split(" ")(1))),s.split(" ")(2)).mkString("-"))
    } catch {
      case _ : Throwable => "December 31, 1900"
    }
  }

  def getTimestampString_udf = udf(getTimestampString _)

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
   else {return sequence.last}
  }

  def main (args: Array[String]) {

    val t0 = System.nanoTime()
    val spark = SparkSession
      .builder()
      .appName("LatestVersionExtracter")
      .config("spark.shuffle.service.enabled","true")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.json("/user/alexeys/metadata/metNY.json").select("filePath","versionDate","version").as[Metadata]
    val data_w_timestamps = data.withColumn("timestamp_string",getTimestampString(col("versionDate"))).withColumn("timestamp", getTimestamp) //drop("")
    val ready_to_join = data_w_timestamps.withColumn("primary_key_to_remove",getPK($"filePath",$"version")).select("primary_key_to_remove","version","timestamp").as[(String,String,java.sql.Timestamp)]

    val results = ready_to_join.groupByKey(_._1).mapGroups((id,iterator)=>(id,iterator.toList.sortWith(_._3.getTime < _._3.getTime).map(_._2))).map{case (x,y) => (x,getLatest(y))}.toDF("pk","latest")
    results.show(40,false)


    //get raw data in the current format
    val raw = spark.read.json("file:///scratch/network/alexeys/bills/lexs/bills_combined_50_p*.json").withColumn("customPK",customPK(col("primary_key")))    
    val output = raw.join(results,raw.col("customPK") === results.col("pk"))

    //for (d <- results.take(10)) {
    //    println(d)
    //}
    output.write.json("/user/alexeys/bills_combined_raw_with_latest_50p1p2") 
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }
} 
case class Metadata(filePath: String,versionDate: String, version: String)
