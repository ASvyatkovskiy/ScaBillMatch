package org.princeton.billmatch.utils

import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.dianahep.histogrammar.sparksql._
import org.dianahep.histogrammar._

object Plot {

  def main(args: Array[String]) {

    val params = ConfigFactory.load("plotting")
    run(params)
  }

  def run(params: Config) {

    val spark = SparkSession.builder().appName("PlotComparisons")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.7")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "10g")
      .getOrCreate()

    import spark.implicits._
     
    lazy val rawInput = params.getString("plotting.rawInput")
    lazy val outputJson = params.getString("plotting.outputJson")
    val input = spark.read.parquet(rawInput)
    input.histogrammar(Bin(10, 0, 100, $"similarity")).toJsonFile(outputJson)
    spark.stop()
   }
}
