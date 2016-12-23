/*
Application: MakeLabeledCartesian, produce all the pairs of primary keys of the documents satisfying a predicate.
Perform document bucketing using k-means clustering.

Following are the key parameters that need to be filled in the resources/makeCartesian.conf file:
	docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted...
        useLSA: whether to use truncated SVD
        numConcepts: number of concepts to use for LSA
        kval: number of clusters for k-means
        onlyInOut: a switch between in-out of state and using both in-out and in-in state pairs
	use_strict: boolean, yes or no to consider strict parameters
	inputFile: input file, one JSON per line
	outputFile: output file
        outputParquetFile: output parquet sink

Example to explore output in spark-shell:
$ spark-shell --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar 
scala> val mydata = sc.objectFile[CartesianPair]("/user/path/to/files")
mydata: org.apache.spark.rdd.RDD[CartesianPair] = MapPartitionsRDD[3] at objectFile at <console>:27

scala> mydata.take(5)
res1: Array[CartesianPair] = Array()
*/
import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

//import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
//import org.apache.spark.ml.clustering.{KMeans, BisectingKMeans}
//import org.apache.spark.mllib.clustering.KMeans

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
//import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.RowMatrix

//we have to deal with this nonsense for now
//import org.apache.spark.mllib.linalg.{
//  Vector => OldVector, 
//  Vectors => OldVectors, 
//  SparseVector => OldSparseVector,
//  DenseVector => OldDenseVector,
//  VectorUDT => OldVectorUDT}

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

//import org.apache.spark.ml.linalg.{
//   Vector => NewVector,
//   Vectors => NewVectors,
//   DenseVector => NewDenseVector,
//   SparseVector => NewSparseVector
//}

import java.io._
//import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertions._


object MakeLabeledCartesian {

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit --class MakeLabeledCartesian --master yarn --queue production --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("makeCartesian")

    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def compactSelector_udf = udf((s: String) => {

       val probe = s.toLowerCase()

       val compactPattern = "compact".r
       val isCompact = compactPattern.findFirstIn(probe).getOrElse("")

       val uniformPattern = "uniform".r
       val isUniform = uniformPattern.findFirstIn(probe).getOrElse("")

       (isCompact.isEmpty() && isUniform.isEmpty())
    })

  def run(params: Config) {

    val spark = SparkSession.builder().appName("MakeLabeledCartesian")
      //.config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.6")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "10g")
      .getOrCreate()

    import spark.implicits._

    val vv: String = params.getString("makeCartesian.docVersion") //like "Enacted"
    val nGramGranularity = params.getInt("makeCartesian.nGramGranularity")
    val addNGramFeatures = params.getBoolean("makeCartesian.addNGramFeatures")
    val numTextFeatures = params.getInt("makeCartesian.numTextFeatures")
    val useLSA = params.getBoolean("makeCartesian.useLSA")
    val kval = params.getInt("makeCartesian.kval")

    val input = spark.read.json(params.getString("makeCartesian.inputFile")).filter($"docversion" === vv).filter(compactSelector_udf(col("content")))
    input.printSchema()
    input.show()

    val npartitions = (4*input.count()/1000).toInt
    val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content"))
    bills.explain

    var rescaled_df = Utils.extractFeatures(bills,numTextFeatures,addNGramFeatures,nGramGranularity)
    rescaled_df = rescaled_df.cache()

    val clusters_schema = StructType(Seq(StructField("primary_key",StringType,false),StructField("docversion",StringType, false),StructField("docid",StringType,false),StructField("state",LongType,false),StructField("year",LongType,false),StructField("features", VectorType, false),StructField("prediction",LongType,false)))
    var clusters_df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], clusters_schema)

    if (useLSA) {
        //Apply low-rank matrix factorization via SVD approach, truncate to reduce the dimensionality
        val dataPart2 = rescaled_df.select("primary_key","docversion","docid","state","year").rdd.map(row => Utils.converted3(row.toSeq)).zipWithIndex().map(x => (x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._2)).toDF("primary_key","docversion","docid","state","year","id")
        //val dataPart2 = rescaled_df.select("primary_key","docversion","docid","state","year").withColumn("id",monotonically_increasing_id)

        val dataRDD = rescaled_df.select("features").rdd.map(row => Utils.converted2(row.toSeq)).map(x => Utils.toOld(x)).cache()

        // Compute the top 5 singular values and corresponding singular vectors.
        //320 concepts worked perfectly for 10 states
        val numConcepts = params.getInt("makeCartesian.numConcepts")
        rescaled_df = Utils.LSA(spark,dataRDD,numConcepts,numConcepts)
        rescaled_df.show()
        rescaled_df.printSchema
        //assert(dataPart2.count() == rescaled_df.count())

        clusters_df = Utils.KMeansSuite(rescaled_df,kval)

        //Setup for splitting by cluster on the step2
        val dataPart1 = clusters_df.select("prediction","features").rdd.map(row => Utils.converted4(row.toSeq)).zipWithIndex().map(x => (x._1._1,x._1._2,x._2)).toDF("prediction","features","id")
        //val dataPart1 = clusters_df.select("prediction").withColumn("id",monotonicallyIncreasingId)

        clusters_df = dataPart1.join(dataPart2, dataPart2("id") === dataPart1("id"))
        //assert(dataPart1.count() == clusters_df.count())

        clusters_df.printSchema()
        clusters_df.show()
        dataPart1.unpersist()
        dataPart2.unpersist()
    } else {
        clusters_df = Utils.KMeansSuite(rescaled_df,kval)
    }
 
    clusters_df.select("primary_key","docversion","docid","state","year","prediction","features").write.parquet(params.getString("makeCartesian.outputParquetFile"))

    var bills_meta = clusters_df.select("primary_key","docversion","docid","state","year","prediction").as[MetaLabeledDocument].cache()
    var bills_meta_bcast = spark.sparkContext.broadcast(bills_meta.collect())

    val strict_params = (params.getBoolean("makeCartesian.use_strict"),params.getInt("makeCartesian.strict_state"),params.getString("makeCartesian.strict_docid"),params.getInt("makeCartesian.strict_year"))

    //will be array of tuples, but the keys are unique
    var cartesian_pairs = bills_meta.rdd.coalesce(params.getInt("makeCartesian.nPartitions"))
                          .map(x => Utils.pairup(x,bills_meta_bcast, strict_params, params.getBoolean("makeCartesian.onlyInOut")))
                          .filter({case (dd,ll) => (ll.length > 0)})
                          .map({case(k,v) => v}).flatMap(x => x) //.groupByKey()    

    cartesian_pairs.saveAsObjectFile(params.getString("makeCartesian.outputFile"))
    spark.stop()
   }
}

case class MetaLabeledDocument(primary_key: String, prediction: Long, state: Long, docid: String, docversion: String, year: Long)
case class CartesianPair(pk1: String, pk2: String)
