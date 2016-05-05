import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

//import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

object MakeCartesian {

  case class Params(inputFile: String = null, docVersion: String = null, nPartitions: Int = 0)
    extends AbstractParams[Params]

  def pairup (document: MetaDocument, thewholething: org.apache.spark.broadcast.Broadcast[Array[MetaDocument]]) : (MetaDocument, Array[CartesianPair]) = {

    val documents = thewholething.value

    val idocversion = document.docversion
    val istate = document.state
    val iyear = document.year
    val pk1 = document.primary_key

    //var output_premap: Tuple2(MetaDocument, Array[CartesianPair]) = Tuple2()
    var output_arr: ArrayBuffer[CartesianPair] = new ArrayBuffer[CartesianPair]()

    for (jevent <- documents) {
       val jdocversion = jevent.docversion
       val jstate = jevent.state
       val jyear = jevent.year
       val pk2 = jevent.primary_key
       if (istate != jstate && iyear < jyear) {
           var output: CartesianPair = CartesianPair(pk1,pk2)
           output_arr += output
       }
     }
     (document,output_arr.toArray)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    val t0 = System.nanoTime()

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MakeCartesian") {
      head("MakeCartesian: an app. that makes up all valid document pairs given selection requirements")
      opt[String]("docVersion")
        .required()
        .text(s"Document version: consider document pairs having a specific version. E.g. Introduced, Enacted...")
        .action((x, c) => c.copy(docVersion = x))
      opt[Int]("nPartitions")
        .required()
        .text(s"Number of partitions in bills_meta RDD")
        .action((x, c) => c.copy(nPartitions = x))
      arg[String]("<inputFile>")
        .required()
        .text(s"input file, one JSON per line, located in NFS by default (file://)")
        .action((x, c) => c.copy(inputFile = x))
      note(
        """
          |For example, the following command runs this app on a dataset:
          |
          | spark-submit  --class MakeCartesian \
          | target/scala-2.10/BillAnalysis-assembly-1.0.jar \
          | --docVersion Enacted --nPartitions 30 /scratch/network/alexeys/bills/lexs/bills_metadata_3.json
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

    val conf = new SparkConf().setAppName("MakeCartesian")
      //.set("spark.driver.maxResultSize", "10g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 

    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val vv: String = params.docVersion //"Enacted"
    var bills_meta = sqlContext.read.json("file://"+params.inputFile).as[MetaDocument].filter(x => x.docversion contains vv).cache()

    var bills_meta_bcast = spark.broadcast(bills_meta.collect())

    //will be array of tuples, but the keys are unique
    var cartesian_pairs = bills_meta.rdd.repartition(params.nPartitions)
                          .map(x => pairup(x,bills_meta_bcast))
                          .filter({case (dd,ll) => (ll.length > 0)})
                          .map({case(k,v) => v}).flatMap(x => x) //.groupByKey()    

    cartesian_pairs.saveAsObjectFile("/user/alexeys/test_object0")

    spark.stop()
   }
}

@serializable case class MetaDocument(primary_key: Long, state: Long, docversion: String, year: Long)
