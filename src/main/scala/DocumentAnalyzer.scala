/*
Application: DocumentAnalyzer, calculate all-pairs similarity considering all the possible combinations of eligible pairs.

Following parameters need to be filled in the resources/documentAnalyzer.conf file:
	secThreshold: Minimum Jaccard similarity to inspect on section level 
        inputBillsFile: Bill input file, one JSON per line
        inputPairsFile: CartesianPairs object input file
        outputMainFile: outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[String,String],Double]
        outputFilteredFile: CartesianPairs passing similarity threshold

Example to explore output in spark-shell:
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object DocumentAnalyzer {

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit --class DocumentAnalyzer --master yarn-client --num-executors 40 --executor-cores 2 --executor-memory 15g target/scala-2.10/BillAnalysis-assembly-1.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("documentAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }


  def run(params: Config) {

    val conf = new SparkConf().setAppName("DocumentAnalyzer")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")

    val spark = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    var bills = sqlContext.read.json(params.getString("documentAnalyzer.inputBillsFile")).as[Document]
    //for more than 30 states, bump over 2000 partitions
    bills = bills.repartition(2005) 

    //First, run the hashing step here
    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => DocumentLevelUtils.preprocess(content)).cache()
    val cartesian_pairs = spark.objectFile[CartesianPair](params.getString("documentAnalyzer.inputPairsFile")).map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues(pp => DocumentLevelUtils.extractSimilarities(pp)) //.cache()
    
    //matches.collect().foreach(println)
    matches.saveAsObjectFile(params.getString("documentAnalyzer.outputMainFile"))

    //scala.Tuple2[String, String]
    val threshold = params.getDouble("documentAnalyzer.secThreshold")
    matches.filter(kv => (kv._2 > threshold)).keys.saveAsObjectFile(params.getString("documentAnalyzer.outputFilteredFile"))

    spark.stop()
  }
}
