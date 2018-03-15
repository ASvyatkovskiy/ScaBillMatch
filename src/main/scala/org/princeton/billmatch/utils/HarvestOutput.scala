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
  lazy val specific_class = params.getString("harvester.app")
  if (specific_class == "workflow2") {
     val specific_params = ConfigFactory.load(specific_class)
     lazy val path = specific_params.getString(specific_class+".outputFileBase")

     // Loads data.
     val input = spark.read.parquet(path+"*_*")
     input.write.parquet(path) 
  } else {
    val specific_params = ConfigFactory.load(specific_class+"_billAnalyzer")
    var path = specific_params.getString(specific_class+"_billAnalyzer.outputMainFile")
    val p = path.split("_").dropRight(1)
    path = p.mkString("_")

    val odata = spark.sparkContext.objectFile[Tuple2[Tuple2[String,String],Float]](path+"_*")
    val odata_df = odata.map(x=>(x._1._1,x._1._2,x._2)).toDF("pk1","pk2","similarity")
    odata_df.write.parquet(path)
  }
 }
}
