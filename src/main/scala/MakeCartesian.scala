import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
//import scala.collection.immutable.Map
//import org.apache.spark.mllib.linalg.Vector

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.storage.StorageLevel

object MakeCartesian {

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
    val conf = new SparkConf().setAppName("MakeCartesian")
      //.set("spark.driver.maxResultSize", "10g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 
    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val vv: String = "Enacted"
    var bills_meta = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_metadata.json").as[MetaDocument].filter(x => x.docversion contains vv)

    var bills_meta_bcast = spark.broadcast(bills_meta.collect())

    //will be array of tuples, but the keys are unique
    var cartesian_pairs = bills_meta.rdd.repartition(30)
                          .map(x => pairup(x,bills_meta_bcast))
                          .filter({case (dd,ll) => (ll.length > 0)})
                          .map({case(k,v) => v}).flatMap(x => x) //.groupByKey()    

    //println(cartesian_pairs.count())
    //println(cartesian_pairs.filter({case (dd,ll) => (ll.length > 0)}).count())

    //Array[scala.Tuple2[MetaDocument, Array[CartesianPair]]] 
    //val mm = cartesian_pairs.collect()
    ////println(manOf(mm))
    //for (element <- mm) {
    //   println(element)
    //}
    //mm.foreach((t2) => println (t2._1 + "-->" + t2._2))

    cartesian_pairs.saveAsObjectFile("/user/alexeys/test_object")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}

case class MetaDocument(primary_key: Long, state: Long, docversion: String, year: Long)
//case class CartesianPair(pk1: Long, pk2: Long)
