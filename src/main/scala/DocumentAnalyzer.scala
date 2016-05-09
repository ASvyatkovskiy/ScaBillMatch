import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object DocumentAnalyzer {

  case class Params(inputFile: String = null, secThreshold: Double = 70.0)
    extends AbstractParams[Params]

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    val t0 = System.nanoTime()

    val defaultParams = Params()

    val parser = new OptionParser[Params]("DocumentAnalyzer") {
      head("MakeCartesian: an app. that makes up all valid document pairs given selection requirements")
      opt[Double]("secThreshold")
        .required()
        .text(s"Minimum Jaccard similarity to inspect on section level")
        .action((x, c) => c.copy(secThreshold = x))
      arg[String]("<inputFile>")
        .required()
        .text(s"input file, one JSON per line, located in NFS by default (file://)")
        .action((x, c) => c.copy(inputFile = x))
      note(
        """
          |For example, the following command runs this app on a dataset:
          |
          | spark-submit  --class DocumentAnalyzer \
          | --master yarn-client --num-executors 40 --executor-cores 2 --executor-memory 15g \
          | target/scala-2.10/BillAnalysis-assembly-1.0.jar \
          | --secThreshold 70.0 /scratch/network/alexeys/bills/lexs/bills_3.json
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }


  def run(params: Params) {

    val conf = new SparkConf().setAppName("DocumentAnalyzer")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 

    val spark = new SparkContext(conf)
    //spark.addJar("file:///home/alexeys/PoliticalScienceTests/ScaBillMatch/target/scala-2.10/BillAnalysis-assembly-1.0.jar")

    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    var bills = sqlContext.read.json("file://"+params.inputFile).as[Document]
    //for more than 30 states, bump over 2000 partitions
    bills = bills.repartition(2005) 

    //First, run the hashing step here
    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => DocumentLevelUtils.preprocess(content)).cache()
    val cartesian_pairs = spark.objectFile[CartesianPair]("/user/alexeys/test_object").map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues(pp => DocumentLevelUtils.extractSimilarities(pp)) //.cache()

    matches.saveAsObjectFile("/user/alexeys/test_main_output")

    //scala.Tuple2[Long, Long]
    matches.filter(kv => (kv._2 > params.secThreshold)).keys.saveAsObjectFile("/user/alexeys/test_new_filtered_pairs")

    spark.stop()
  }
}
@serializable case class Document(primary_key: Long, content: String)
@serializable case class CartesianPair(pk1: Long, pk2: Long)
