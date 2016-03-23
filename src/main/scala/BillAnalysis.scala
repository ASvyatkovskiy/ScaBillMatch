import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
//import org.apache.spark.mllib.linalg.Vector

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray


object BillAnalysis {

  def preprocess (line: String) : ArrayBuffer[Long] = {
    val distinct_tokens = Stemmer.tokenize(line).distinct //.mkString
    //println(distinct_tokens)

    var wGrps = ArrayBuffer.empty[Long]
    val grpSize = 5
    for (n <- 0 to distinct_tokens.length-grpSize) {
      val cgrouplst = distinct_tokens.drop(n).take(grpSize)
      var cgrp = ""
      //var cgrp = " ".join(cgrouplst)
      for (tok <- cgrouplst) {
         cgrp += tok
      }
      wGrps += cgrp.hashCode()
    }
    wGrps.distinct
  }


  def extractSimilarities = (s_wGrps: WrappedArray[Long], m_wGrps: WrappedArray[Long]) => {
    val model_size = (m_wGrps.length+s_wGrps.length)/2.
    val matchCnt = m_wGrps.intersect(s_wGrps).length
    matchCnt/model_size * 100.
  }

  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("TextProcessing")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    //register UDFs
    sqlContext.udf.register("df_preprocess",preprocess _)
    val df_preprocess_udf = udf(preprocess _)
    sqlContext.udf.register("df_extractSimilarities",extractSimilarities)
    val df_extractSimilarities_udf = udf(extractSimilarities)

    var bills = sqlContext.read.json("file:///home/alexeys/PoliticalScienceTests/FreshStart_Tests/bills.json")
    //bills.printSchema()
   
    //apply necessary UDFs
    bills = bills.withColumn("hashed_content", df_preprocess_udf(col("content"))).drop("content")
    //bills = bills.withColumn('hashed_content', df_extractHashedSets_udf(col("good_content"))).drop("good_content")
    bills.registerTempTable("bills_df")

    val cartesian = sqlContext.sql("SELECT a.primary_key as pk1, b.primary_key as pk2, a.hashed_content as hc1, b.hashed_content as hc2 FROM bills_df a, bills_df b WHERE a.primary_key != b.primary_key AND a.year <= b.year AND a.state != b.state")
    //cartesian.printSchema()
    println(cartesian.count())
    //val matches = cartesian.withColumn("similarities", df_extractSimilarities_udf(col("hc1"),col("hc2"))).select("similarities","pk1","pk2")
    val matches = cartesian.withColumn("similarities", df_extractSimilarities_udf(col("hc1"),col("hc2"))).select("similarities")
    //matches.printSchema() 

    //for (word <- matches.collect()) {
    //  println(word)
    //}
    //println(matches.count())

    matches.write.parquet("/user/alexeys/test_output2")
    //matches.write.save("/user/alexeys/test_output2")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}
