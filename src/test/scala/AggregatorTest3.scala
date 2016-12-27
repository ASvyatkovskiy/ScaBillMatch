import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable._

import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import org.apache.spark.RangePartitioner

import org.princeton.billmatch.feature._
import org.princeton.billmatch.similarity._
import org.princeton.billmatch._

object AggregatorTest3 {

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main (args: Array[String]) {

    val t0 = System.nanoTime()

    val spark = SparkSession
      .builder()
      .appName("DatasetAggregatorTest")
      //.config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.5")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val bills = spark.read.parquet("/user/alexeys/bills_combined_3/part*").cache()

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

    var ds = rescaled_df.select("primary_key","state","prediction","features").as[AggDocument].rdd.map(x=>(x.prediction,(x.primary_key,x.state,x.features))).coalesce(150)

    var similarityMeasure: SimilarityMeasure = CosineSimilarity
    val zero = ListBuffer.empty[(String,Long,Vector)]
    //what you do within partition
    def my_grouper = (s: ListBuffer[(String,Long,Vector)], v: (String,Long,Vector))  => s += v
    //what you do between partitions
    def my_combiner = (p1: ListBuffer[(String,Long,Vector)], p2: ListBuffer[(String,Long,Vector)]) => p1 ++= p2

    //mapValues(x => x.combinations(2).filter{case Seq(x,y) => (x._2 != y._2)}.map{case Seq(x,y) => (x._1,y._1,similarityMeasure.compute(x._3.toSparse,y._3.toSparse))}.
    //def mapPartitions2(iter: Iterator[(Long,ListBuffer[(String,Long,Vector)])]): Iterator[(String, String, Double)] = {
    //    var result = List.empty[(String, String, Double)]
    //    if (iter.hasNext) {
    //       val x: ListBuffer[(String,Long,Vector)] = iter.next._2
    //       result = x.combinations(2).filter{case Seq(x,y) => (x._2 != y._2)}.map{case Seq(x,y) => (x._1,y._1,similarityMeasure.compute(x._3.toSparse,y._3.toSparse))}.filter{case (x,y,z) => (z > 70.0)}.toList //.toIndexedSeq 
    //    }
    //    result.toIterator
    //}

    val rangeP = new RangePartitioner(150,ds) 
    val results = ds.partitionBy(rangeP).aggregateByKey(zero)(my_grouper,my_combiner).mapValues(x => x.combinations(2).filter{case Seq(x,y) => (x._2 != y._2)}.map{case Seq(x,y) => (x._1,y._1,similarityMeasure.compute(x._3.toSparse,y._3.toSparse))}.filter{case (x,y,z) => (z > 70.0)}.toIndexedSeq).filter(x => (x._2.length != 0)).flatMap(x=>x._2).toDF("pk1","pk2","similarity")
    //val results = ds.aggregateByKey(zero)(my_grouper,my_combiner).mapPartitions(mapPartitions2).toDF("pk1","pk2","similarity")

    //results.printSchema()
    //results.show()

    //for (d <- results.take(10)) {
    //    println(d)
    //}
    results.write.parquet("/user/alexeys/TEST") 

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }

} 
//case class AggDocument(primary_key: String, prediction: Long, state: Long, features: Vector)
