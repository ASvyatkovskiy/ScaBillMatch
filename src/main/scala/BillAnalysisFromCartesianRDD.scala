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

import org.apache.spark.storage.StorageLevel

object BillAnalysisFromCartesianRDD {

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

  def extractSimilarities = (grps: Tuple2[ArrayBuffer[Long], ArrayBuffer[Long]]) => {
    val s_wGrps = grps._1
    val m_wGrps = grps._2
    val model_size = (m_wGrps.length+s_wGrps.length)/2.
    val matchCnt = m_wGrps.intersect(s_wGrps).length
    matchCnt/model_size * 100.
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("TextProcessing")
      .set("spark.driver.maxResultSize", "10g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 
    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val bills = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_small.json").as[Document]

    //First, run the hashing step here
    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => preprocess(content))

    val cartesian_pairs = sqlContext.read.json("file:///home/alexeys/PoliticalScienceTests/ScaBillMatch/dataformat/good_pairs_small.json").as[CartesianPair].rdd.map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2), (v1, v2))}).distinct.mapValues(pp => extractSimilarities(pp))
        //.map({case(_, (((k1,k2), v1), v2))=> ((k1,k2),extractSimilarities(v1, v2))})

    for (m <- matches) {
        println(m) 
    } 

    //matches.write.parquet("/user/alexeys/test_output")
    //var opath_str = sys.env("PWD")
    //opath_str = "file://".concat(opath_str).concat("/test_output")
    //matches.printSchema() 

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}

case class Document(primary_key: Long, content: String)
case class CartesianPair(pk1: Long, pk2: Long)
