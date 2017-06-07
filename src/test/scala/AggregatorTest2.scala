import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import org.princeton.billmatch.feature._
import org.princeton.billmatch.similarity._
import org.princeton.billmatch._

object AggregatorTest2 {

  class SeqAgg(private val similarityMeasure: SimilarityMeasure) extends Aggregator[AggDocument, Seq[(String,Long,Vector)], Seq[(String,String,Float)]] {
     def zero: Seq[(String,Long,Vector)] = Nil //this should have a type of merge buffer
     def reduce(b: Seq[(String,Long,Vector)], a: AggDocument): Seq[(String,Long,Vector)] = (a.primary_key,a.state,a.features) +: b
     def merge(b1: Seq[(String,Long,Vector)], b2: Seq[(String,Long,Vector)]): Seq[(String,Long,Vector)] = b1 ++ b2
     def finish(r: Seq[(String,Long,Vector)]): Seq[(String,String,Float)] = { 
         val buffy = r.combinations(2).filter{case Seq(x,y) => (x._2 != y._2)}.map{case Seq(x,y) => (x._1,y._1,similarityMeasure.compute(x._3.toSparse,y._3.toSparse))}.filter{case (x,y,z) => (z > 70.0)}.toIndexedSeq
         //println(buffy.length)
         buffy
     }
     override def bufferEncoder: Encoder[Seq[(String,Long,Vector)]] = ExpressionEncoder()
     override def outputEncoder: Encoder[Seq[(String,String,Float)]] = ExpressionEncoder()
  }

  def main (args: Array[String]) {

    val t0 = System.nanoTime()

    val spark = SparkSession
      .builder()
      .appName("DatasetAggregatorTest")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val bills = spark.read.parquet("/user/alexeys/part-r-*").cache()   //part-r-00846-673e69d6-d337-445c-9de0-e450281972b7.snappy.parquet").cache()

    println(bills.count())
    println(bills.rdd.getNumPartitions)
    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))).drop("content")

    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    var prefeaturized_df = remover.transform(tokenized_df).drop("words")
    prefeaturized_df = prefeaturized_df.select(col("primary_key"),col("state"),col("prediction"),col("filtered").alias("combined"))
    var hashingTF = new HashingTF().setInputCol("combined").setOutputCol("rawFeatures").setNumFeatures(16384)
    val featurized_df = hashingTF.transform(prefeaturized_df)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")

    var ds = rescaled_df.select("primary_key","state","prediction","features").as[AggDocument]
    val nparts: Int = ds.rdd.getNumPartitions
    ds = ds.repartition(nparts*50).cache()
    ds.explain
    ds.printSchema()
    ds.show()
    println(ds.count()) 

    var similarityMeasure: SimilarityMeasure = CosineSimilarity
    val myAgg = new SeqAgg(similarityMeasure)
    def countfeat_udf = udf((s: WrappedArray[Any]) => (s.length != 0))

    val results = ds.groupByKey(_.prediction).agg(myAgg.toColumn).toDF("prediction","similarity_kkv").drop("prediction").filter(countfeat_udf(col("similarity_kkv"))).cache() 

    //.rdd.flatMapValues(x => x).toDF("prediction","similarity_kkv").drop("prediction")

    results.printSchema()
    results.show()

    //for (d <- results.take(10)) {
    //    println(d)
    //}
    results.write.parquet("/user/alexeys/new_3_state_test") 

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }

} 
case class AggDocument(primary_key: String, prediction: Long, state: Long, features: Vector)
