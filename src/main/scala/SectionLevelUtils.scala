import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object SectionLevelUtils {

  def preprocess (line: String) : ArrayBuffer[ArrayBuffer[Long]] = {

    val sectionPattern = "(SECTION \\d|section \\d)"
    val distinct_sections: Array[String] = line.split(sectionPattern)
    var combined_wGrps = ArrayBuffer.empty[ArrayBuffer[Long]]

    //for each section
    for (sec <- distinct_sections) {
        val wGrps: ArrayBuffer[Long] = DocumentLevelUtils.preprocess(sec)
        combined_wGrps += wGrps
    }
    combined_wGrps
  }

  //calculate similarities for each sections (double for loop kind of thing)
  def extractSimilarities = (section_grps: Tuple2[ArrayBuffer[ArrayBuffer[Long]], ArrayBuffer[ArrayBuffer[Long]]]) => {

    var matchCnt = ArrayBuffer.empty[Double]
    val igrp = section_grps._1
    val jgrp = section_grps._2
    for (isec <- 0 to igrp.length-1) {
       for (jsec <- isec to jgrp.length-1) {
           matchCnt += DocumentLevelUtils.extractSimilarities((igrp(isec),jgrp(jsec)))
       } 
    } 
    matchCnt.reduceLeft(_ max _)
  }
}
