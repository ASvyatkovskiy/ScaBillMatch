package org.princeton.billmatch.utils

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession

import com.typesafe.config._
import org.princeton.billmatch.stats._

object Postprocessor { 

 def main(args: Array[String]) {

  val spark = SparkSession.builder().appName("Postprocessor")
    .config("spark.driver.maxResultSize", "10g")
    .getOrCreate()

  import spark.implicits._

  val params = ConfigFactory.load("postprocessor")
  lazy val inputJsonPath = params.getString("postprocessor.inputJsonPath")
  lazy val inputResultParquet = params.getString("postprocessor.inputResultParquet")
  lazy val getNBest = params.getInt("postprocessor.getNBest") 
  lazy val isAscending = params.getBoolean("postprocessor.isAscending")
  lazy val outputSkimFile = params.getString("postprocessor.outputSkimFile")
  lazy val outputLightFile = params.getString("postprocessor.outputLightFile") 

  //takes the output of the steps2 of the bill analysis
  //val ordered_skim_data = AnalysisUtils.sampleNOrdered(spark,inputJsonPath,inputResultParquet,getNBest,isAscending,true)

  //saves to the skimmed
  //ordered_skim_data.repartition(1).write.json(outputSkimFile)

  //to get light specify
  val raw = spark.read.parquet(inputResultParquet) 
  val ordered_light_data = AnalysisUtils.makeLight(raw)

  //saves to the light
  ordered_light_data.write.json(outputLightFile)

 }
}
