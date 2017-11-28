package org.princeton.billmatch.utils

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession

import com.typesafe.config._

object HarvestOutput { 

 def main(args: Array[String]) {

  val spark = SparkSession.builder().appName("HarvestOutput")
    .config("spark.driver.maxResultSize", "10g")
    .getOrCreate()

  import spark.implicits._

  val params = ConfigFactory.load("harvester")
  val specific_class = params.getString("harvester.app")
  val specific_params = ConfigFactory.load(specific_class)
  if (specific_class == "workflow2") {
     val path = specific_params.getString(specific_class+".outputFileBase")

     // Loads data.
     val input = spark.read.parquet(path+"*_*")
     input.write.parquet(path) 
  } else {
    val odata = spark.sparkContext.objectFile[Tuple2[Tuple2[String,String],Double]](specific_class+"_billAnalyzer.outputMainFile")
    val odata_df = skimmed_data.map(x=>(x._1._1,x._1._2,x._2)).toDF("pk1","pk2","similarity")
    odata_df.write.parquet(specific_class+"_billAnalyzer.outputMainFile"+"_df")
  }
 }
}
