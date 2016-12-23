import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry,RowMatrix}

import org.apache.spark.mllib.linalg.{
  Vector => OldVector,
  Vectors => OldVectors,
  SparseVector => OldSparseVector,
  DenseVector => OldDenseVector,
  VectorUDT => OldVectorUDT}

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.linalg.{
   Vector => NewVector,
   Vectors => NewVectors,
   DenseVector => NewDenseVector,
   SparseVector => NewSparseVector
}

import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertions._


object DIMSUM {

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
      .config("spark.shuffle.memoryFraction","0.5")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "10g")
      .getOrCreate()

    import spark.implicits._

    val vv: String = params.getString("makeCartesian.docVersion") //like "Enacted"
    val input = spark.read.json(params.getString("makeCartesian.inputFile")).filter($"docversion" === vv).filter(compactSelector_udf(col("content")))
    input.printSchema()
    input.show()

    //val npartitions = (4*input.count()/1000).toInt
    //FIXME this line messes up the order
    //val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content"))
    //bills.explain
    val bills = input 

    val nGramGranularity = params.getInt("makeCartesian.nGramGranularity")
    val addNGramFeatures = params.getBoolean("makeCartesian.addNGramFeatures")
    val numTextFeatures = params.getInt("makeCartesian.numTextFeatures")
    val useLSA = params.getBoolean("makeCartesian.useLSA")
    val kval = params.getInt("makeCartesian.kval")
    var rescaled_df = Utils.extractFeatures(bills,numTextFeatures,addNGramFeatures,nGramGranularity)
    val dataRDD = rescaled_df.select("features").rdd.map {
          case Row(v: NewVector) => OldVectors.fromML(v)}.cache()

    val numConcepts = params.getInt("makeCartesian.numConcepts")
    val reconstructedT: RowMatrix = Utils.LSAmatrix(spark,dataRDD,numConcepts,2*numConcepts)
    //val transposed = Utils.transposeRowMatrix(mat)
    val approx = Utils.DIMSUMSuite(reconstructedT,1.0)

    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }.map(x => x.swap).sortByKey(false).map(x => (x._2._1,x._2._2,x._1)).toDF("pk1","pk2","similarity")
    approxEntries.show()
    approxEntries.write.parquet(params.getString("makeCartesian.outputParquetFile"))

    //get back descriptive labels
    //primary_key -> state+"_"+year+"_"+bill+"_"+docversion

    spark.stop()
   }
}
