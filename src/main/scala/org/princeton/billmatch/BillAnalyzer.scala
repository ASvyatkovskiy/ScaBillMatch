package org.princeton.billmatch

/*BillAnalyzer: an app. that performs document or section all=pairs similarity starting off CartesianPairs

Following are the key parameters need to be filled in the resources/billAnalyzer.conf file:
    measureName: Similarity measure used
    inputParquetFile: Parquet file with features
    inputPairsFile: CartesianPairs object input file
    outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[String,String],Double]
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.collection.mutable.WrappedArray

import org.princeton.billmatch.feature._
import org.princeton.billmatch.similarity._

object BillAnalyzer {

  def converted(row: scala.collection.Seq[Any]) : Tuple2[String,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[String]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit --class BillAnalyzer --master yarn --queue production --num-executors 40 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("billAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def run(params: Config) {

    val spark = SparkSession
      .builder()
      .appName("BillAnalysis")
      //.config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.5")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._
 
    val bills = spark.read.parquet(params.getString("billAnalyzer.inputParquetFile")).coalesce(params.getInt("billAnalyzer.nPartitions")).cache()
    
    val hashed_bills = bills.select("primary_key","features").rdd.map(row => converted(row.toSeq)).cache()

    val cartesian_pairs = spark.sparkContext.objectFile[CartesianPair](params.getString("billAnalyzer.inputPairsFile"),params.getInt("billAnalyzer.nCPartitions")).map(pp => (pp.pk1,pp.pk2))

    var similarityMeasure: SimilarityMeasure = null
    var threshold: Double = 0.0

    params.getString("billAnalyzer.measureName") match {
      case "cosine" => {
        similarityMeasure = CosineSimilarity
        threshold = -20.0
      }
      case "hamming" => {
        similarityMeasure = HammingSimilarity
        threshold = -0.02
      }
      case "manhattan" => {
        similarityMeasure = ManhattanSimilarity
        threshold = -0.02
      }
      case "jaccard" => {
        similarityMeasure = JaccardSimilarity
        threshold = -20.0
      }
      case "weighted_jaccard" => {
        similarityMeasure = weightedJaccardSimilarity
        threshold = -20.0
      }
      case other: Any =>
        throw new IllegalArgumentException(
          s"Only hamming, cosine, euclidean, manhattan, and jaccard similarities are supported but got $other."
        )
    }

    val firstjoin = Utils.twoSidedJoin(cartesian_pairs,hashed_bills)
    val matches = firstjoin.mapValues({case (v1,v2) => similarityMeasure.compute(v1,v2)}).filter({case (k,v) => (v > threshold)})
    //matches.map(x=>(x._1._1,x._1._2,x._2)).toDF("pk1","pk2","similarity").write.parquet(params.getString("billAnalyzer.outputMainFile"))
    matches.saveAsObjectFile(params.getString("billAnalyzer.outputMainFile"))

    spark.stop()
   }
}
