import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
//import org.apache.spark.mllib.linalg.Vector

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray


object BillAnalysis {

  def preprocess (line: String) : ArrayBuffer[Long] = {
    val distinct_tokens = Stemmer.tokenize(line).distinct //.mkString
    //println(distinct_tokens)

    var wGrps = ArrayBuffer.empty[Long]
    val grpSize = 5
    for (n <- 0 to distinct_tokens.length-grpSize) {
      val cgrouplst = distinct_tokens.drop(n).take(grpSize)
      var cgrp = ""
      //var cgrp = " ".join(cgrouplst)
      for (tok <- cgrouplst) {
         cgrp += tok
      }
      wGrps += cgrp.hashCode()
    }
    wGrps.distinct
  }


  def extractSimilarities = (s_wGrps: WrappedArray[Long], m_wGrps: WrappedArray[Long]) => {
    val model_size = (m_wGrps.length+s_wGrps.length)/2.
    val matchCnt = m_wGrps.intersect(s_wGrps).length
    matchCnt/model_size * 100.
  }

  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("TextProcessing")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.mb","24") 
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    //println(sys.env("PWD"))
    //register UDFs
    sqlContext.udf.register("df_preprocess",preprocess _)
    val df_preprocess_udf = udf(preprocess _)
    sqlContext.udf.register("df_extractSimilarities",extractSimilarities)
    val df_extractSimilarities_udf = udf(extractSimilarities)

    //var ipath_str = sys.env("PWD")
    //ipath_str = "file://".concat(ipath_str).concat("/data/bills_metadata.json")
    var ipath_meta_str = "file:///scratch/network/alexeys/bills/lexs/text_1state/bills_metadata.json"
    var bills_meta = sqlContext.read.json(ipath_meta_str)
    bills_meta.printSchema()
  
    //Repartition: key1 to solving current memory issues: (java heap OutOfMemory)
    bills_meta = bills_meta.repartition(col("primary_key"))
    bills_meta.explain
    bills_meta.cache()
    bills_meta.registerTempTable("bills_meta_df")
    println(bills_meta.count())

    //JOIN on !=
    //val cartesian = sqlContext.sql("SELECT a.primary_key as pk1, b.primary_key as pk2, a.hashed_content as hc1, b.hashed_content as hc2 FROM bills_df a LEFT JOIN bills_df b ON a.primary_key != b.primary_key WHERE a.year < b.year AND a.state != b.state AND a.docversion LIKE 'Enacted%' AND b.docversion LIKE 'Enacted%'")
    val cartesian_meta = sqlContext.sql("SELECT a.primary_key as pk1, b.primary_key as pk2 FROM bills_meta_df a LEFT JOIN bills_meta_df b ON a.primary_key != b.primary_key WHERE a.year < b.year AND a.state != b.state") // AND a.docversion LIKE 'Enacted%' AND b.docversion LIKE 'Enacted%'")
    cartesian_meta.cache()
    val cnt = cartesian_meta.count()
    cartesian_meta.printSchema()
    println(cnt)
    //for deugging
    //for (word <- cartesian_meta.take(2)) {
    //  println(word)
    //  println(word(1)) 
    //}

    if (cnt != 0) {

        cartesian_meta.registerTempTable("cartesian_df")
        //val flat_cartesian = sqlContext.sql("SELECT pk1 FROM cartesian_df UNION SELECT pk2 FROM cartesian_df")
        //flat_cartesian.printSchema()
        //flat_cartesian.show()
        //println(flat_cartesian.distinct.count())
        //flat_cartesian.registerTempTable("flat_cartesian_df")

        var ipath_str = "file:///scratch/network/alexeys/bills/lexs/text_1state/bills.json"
        var bills = sqlContext.read.json(ipath_str)
        bills.printSchema()
  
        //Repartition: key1 to solving current memory issues: (java heap OutOfMemory)
        bills = bills.repartition(col("primary_key"))
        bills.explain
        //bills.cache()
        //bills.registerTempTable("bills_df")
        //println(bills.count())

        //filter by key based on the bills_meta
        //val bills_filtered = sqlContext.sql("SELECT a.primary_key as pk, a.content as cont FROM bills_df a LEFT JOIN flat_cartesian_df b ON a.primary_key = b.pk1")
        //bills_filtered.printSchema()
        //println(bills_filtered.count())

        //apply UDF
        bills = bills.withColumn("hashed_content", df_preprocess_udf(col("content"))).drop("content")
        //bills.cache() 
       
        //FIXME approach

        //l.map(x => x -> (f _).tupled(x)).toMap

        val hashed_bills = bills.rdd.map(x => x(0),List())collect()
        for (pp <- hashed_bills) {
           
           matches.append(extractSimilarities())
        }
        val useful_pairs = cartesian_meta.collect()
        ///var matches = List[Float]
        println(useful_pairs)

        //for (pp <- useful_pairs) {
            //val el1 = hashed_bills_dict(pp(1))
            //val el2 = hashed_bills_dict(pp(2))
            //matches.append(extractSimilarities())
        //}

        //val matches = cartesian.withColumn("similarities", df_extractSimilarities_udf(col("hc1"),col("hc2"))).select("similarities")
        //matches.write.parquet("/user/alexeys/test_output")
        //var opath_str = sys.env("PWD")
        //opath_str = "file://".concat(opath_str).concat("/test_output")
        //matches.printSchema() 
        println("Hi")
    }

    //matches.write.parquet("/user/alexeys/test_output")
    //var opath_str = sys.env("PWD")
    //opath_str = "file://".concat(opath_str).concat("/test_output")
    //matches.write.parquet(opath_str)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}
