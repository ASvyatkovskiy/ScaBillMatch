/*
Application: MakeCartesian, produce all the pairs of primary keys of the documents satisfying a predicate.

Following parameters need to be filled in the resources/makeCartesian.conf file:
	docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted...
	nPartitions: number of partitions in bills_meta RDD
	use_strict: boolean, yes or no to consider strict parameters
	strict_state: specify state (a long integer from 1 to 50) for one-against-all user selection (strict)
	strict_docid: specify document ID for one-against-all user selection (strict)
	strict_year: specify year for one-against-all user selection (strict)
	inputFile: input file, one JSON per line
	outputFile: output file

Example to explore output in spark-shell:
$ spark-shell --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar 
scala> val mydata = sc.objectFile[CartesianPair]("/user/alexeys/valid_pairs")
mydata: org.apache.spark.rdd.RDD[CartesianPair] = MapPartitionsRDD[3] at objectFile at <console>:27

scala> mydata.take(5)
res1: Array[CartesianPair] = Array()
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

//import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray


object MakeCartesian {

  def pairup (document: MetaDocument, thewholething: org.apache.spark.broadcast.Broadcast[Array[MetaDocument]], strict_params: Tuple4[Boolean, Int, java.lang.String, Int]) : (MetaDocument, Array[CartesianPair]) = {

    val documents = thewholething.value

    val (use_strict,strict_state,strict_docid,strict_year) = strict_params

    val idocversion = document.docversion
    val istate = document.state
    val iyear = document.year
    val idocid = document.docid
    val pk1 = document.primary_key

    //var output_premap: Tuple2(MetaDocument, Array[CartesianPair]) = Tuple2()
    var output_arr: ArrayBuffer[CartesianPair] = new ArrayBuffer[CartesianPair]()

    for (jevent <- documents) {
       val jdocversion = jevent.docversion
       val jstate = jevent.state
       val jyear = jevent.year
       val pk2 = jevent.primary_key
       if (use_strict) {
         //extra condition
         if (istate == strict_state && idocid == strict_docid && iyear == strict_year) {
           if (istate != jstate && iyear < jyear) {
              var output: CartesianPair = CartesianPair(pk1,pk2)
              output_arr += output
           }
         } 
       } else {
         //simple condition
          if (istate != jstate && iyear < jyear) {
             var output: CartesianPair = CartesianPair(pk1,pk2)
             output_arr += output
          }
       }
     }
     (document,output_arr.toArray)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit --class MakeCartesian --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("makeCartesian")

    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def run(params: Config) {

    val conf = new SparkConf().setAppName("MakeCartesian")
      //.set("spark.driver.maxResultSize", "10g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 

    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val vv: String = params.getString("makeCartesian.docVersion") //"Enacted"
    var bills_meta = sqlContext.read.json(params.getString("makeCartesian.inputFile")).as[MetaDocument].filter(x => x.docversion contains vv).cache()

    var bills_meta_bcast = spark.broadcast(bills_meta.collect())

    val strict_params = (params.getBoolean("makeCartesian.use_strict"),params.getInt("makeCartesian.strict_state"),params.getString("makeCartesian.strict_docid"),params.getInt("makeCartesian.strict_year"))

    //will be array of tuples, but the keys are unique
    var cartesian_pairs = bills_meta.rdd.repartition(params.getInt("makeCartesian.nPartitions"))
                          .map(x => pairup(x,bills_meta_bcast, strict_params))
                          .filter({case (dd,ll) => (ll.length > 0)})
                          .map({case(k,v) => v}).flatMap(x => x) //.groupByKey()    

    cartesian_pairs.saveAsObjectFile(params.getString("makeCartesian.outputFile"))

    spark.stop()
   }
}

@serializable case class MetaDocument(primary_key: Long, state: Long, docid: String, docversion: String, year: Long)
