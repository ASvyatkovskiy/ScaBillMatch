import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.WrappedArray

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute


object BillAnalysisFromCartesianRDD {

  def preprocess (line: String) : ArrayBuffer[Long] = {
    val distinct_tokens = Stemmer.tokenize(line).distinct //.mkString

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

  def sections_preprocess (line: String) : ArrayBuffer[ArrayBuffer[Long]] = {
    val sectionPattern = "(SECTION \\d|section \\d)"
    val distinct_sections: Array[String] = line.split(sectionPattern)
    var combined_wGrps = ArrayBuffer.empty[ArrayBuffer[Long]]

    //for each section
    for (sec <- distinct_sections) {
        val wGrps: ArrayBuffer[Long] = preprocess(sec)
        combined_wGrps += wGrps
    }
    combined_wGrps
  }


  def extractSimilarities = (grps: Tuple2[ArrayBuffer[Long], ArrayBuffer[Long]]) => {
    val s_wGrps = grps._1
    val m_wGrps = grps._2
    val model_size = (m_wGrps.length+s_wGrps.length)/2.
    val matchCnt = m_wGrps.intersect(s_wGrps).length
    matchCnt/model_size * 100.
  }

  //calculate similarities for each sections (double for loop kind of thing)
  def extractSectionSimilarities = (section_grps: Tuple2[ArrayBuffer[ArrayBuffer[Long]], ArrayBuffer[ArrayBuffer[Long]]]) => {

    var matchCnt = ArrayBuffer.empty[Double]
    val igrp = section_grps._1
    val jgrp = section_grps._2
    for (isec <- 0 to igrp.length-1) {
       for (jsec <- isec to jgrp.length-1) {
           matchCnt += extractSimilarities((igrp(isec),jgrp(jsec)))
       } 
    } 
    matchCnt
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
    spark.addJar("file:///home/alexeys/PoliticalScienceTests/ScaBillMatch/target/scala-2.10/BillAnalysis-assembly-1.0.jar")
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val bills = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_3.json").as[Document]

    //First, run the hashing step here
    //val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => preprocess(content)).cache()
    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => sections_preprocess(content)).cache()

    val cartesian_pairs = spark.objectFile[CartesianPair]("/user/alexeys/test_object").map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>(v1, v2)}).map(pp => extractSectionSimilarities(pp))
    //.map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues(pp => extractSimilarities(pp))
    //.map({case(_, (((k1,k2), v1), v2))=>(v1, v2)}).map(pp => extractSimilarities(pp))
    //    //.map({case(_, (((k1,k2), v1), v2))=> ((k1,k2),extractSimilarities(v1, v2))})

    matches.saveAsTextFile("/user/alexeys/test_output")
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
