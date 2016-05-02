import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.WrappedArray


object DocumentAnalyzer {

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("TextProcessing")
      .set("spark.driver.maxResultSize", "10g")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer.mb","24") 
    val spark = new SparkContext(conf)
    spark.addJar("file:///home/alexeys/PoliticalScienceTests/ScaBillMatch/target/scala-2.10/BillAnalysis-assembly-1.0.jar")
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val bills = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_3.json").as[Document]

    //First, run the hashing step here
    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => DocumentLevelUtils.preprocess(content)).cache()
    val cartesian_pairs = spark.objectFile[CartesianPair]("/user/alexeys/test_object").map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues(pp => DocumentLevelUtils.extractSimilarities(pp))
    //.map({case(_, (((k1,k2), v1), v2))=>(v1, v2)}).map(pp => extractSimilarities(pp))
    //.map({case(_, (((k1,k2), v1), v2))=>((k1,k2),extractSimilarities(v1, v2))})

    //matches.saveAsTextFile("/user/alexeys/test_output")
    matches.saveAsObjectFile("/user/alexeys/test_main_output")

    //val filtered_matches_df = matches_df.filter(matches_df("similarity") > 70.0).select("pre_CartesianPair").show()
    //scala.Tuple2[Long, Long]
    matches.filter(kv => (kv._2 > 70.0)).keys.saveAsObjectFile("/user/alexeys/test_new_filtered_pairs")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}
case class Document(primary_key: Long, content: String)
case class CartesianPair(pk1: Long, pk2: Long)
