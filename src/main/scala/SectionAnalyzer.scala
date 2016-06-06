import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object SectionAnalyzer {

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
 
  def main(args: Array[String]) {

    println(s"\nspark-submit --class SectionAnalyzer --master yarn-client --num-executors 40 --executor-cores 2 --executor-memory 15g target/scala-2.10/BillAnalysis-assembly-1.0.jar\n")

    val t0 = System.nanoTime()
    val params = ConfigFactory.load("sectionAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def run(params: Config) {

    val conf = new SparkConf().setAppName("SectionAnalyzer")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")

    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._

    val bills = sqlContext.read.json(params.getString("sectionAnalyzer.inputBillsFile")).as[Document]

    val hashed_bills = bills.rdd.map(bill => (bill.primary_key,bill.content)).mapValues(content => SectionLevelUtils.preprocess(content)).cache()
    val cartesian_pairs = spark.objectFile[Tuple2[String,String]](params.getString("sectionAnalyzer.inputFilteredFile"))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues(pp => SectionLevelUtils.extractSimilarities(pp))

    matches.saveAsObjectFile(params.getString("sectionAnalyzer.outputMainSectionFile"))

    spark.stop()
   }
}
