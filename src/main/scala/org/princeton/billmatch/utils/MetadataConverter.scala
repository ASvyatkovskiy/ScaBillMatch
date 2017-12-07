package org.princeton.billmatch.utils

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.typesafe.config._
import java.io._

object MetadataConverter {

  def main(args: Array[String]) {

    val t0 = System.nanoTime()

    val spark = SparkSession.builder().appName("MetadataConverter")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "20g")
      .getOrCreate()

    import spark.implicits._

    val params = ConfigFactory.load("metadata")
    val inputJsonBasePath = params.getString("metadata.inputJsonBasePath")
    val outputJsonBasePath = params.getString("metadata.outputJsonBasePath")
    val isFederal = params.getBoolean("metadata.isFederal")

    var states = List("WA", "WI", "WV", "FL", "WY", "NH", "NJ", "NM", "NC", "ND", "NE", "NY", "RI", "NV", "CO", "CA", "GA", "CT", "OK", "OH", "KS", "SC", "KY", "OR", "SD", "DE", "HI", "TX", "LA", "TN", "PA", "VA", "AK", "AL", "AR", "VT", "IL", "IN", "IA", "AZ", "ID", "ME", "MD", "MA", "UT", "MO", "MN", "MI", "MT", "MS").par

    if (isFederal) states = List("h","s").par

    for (ss <- states) {
       println("Processing state "+ss)
       val data = spark.sparkContext.wholeTextFiles(inputJsonBasePath+"/"+ss+"/*/*/*json")
       val d = data.map(x=>x._2).map(x => x.replace("\n", "")).map(x => x.trim().replaceAll(" +", " "))
       val ddd = d.collect().mkString("\n")
       val pw = new PrintWriter(new File(outputJsonBasePath+"/met_"+ss+".json" ))
       pw.write(ddd)
       pw.close()
    }
    spark.stop()

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
   }
}
