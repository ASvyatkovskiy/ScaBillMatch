import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.WrappedArray

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute


object DocumentLevelUtils {

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

  def extractSimilarities (grps: Tuple2[ArrayBuffer[Long], ArrayBuffer[Long]]) : Double = {
    val s_wGrps = grps._1
    val m_wGrps = grps._2
    val matchCnt = m_wGrps.intersect(s_wGrps).length.toDouble
    matchCnt/(m_wGrps.length + s_wGrps.length - matchCnt)*100.
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
}
