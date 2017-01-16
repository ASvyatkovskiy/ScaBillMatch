package org.apache.spark.ml.feature

import com.typesafe.config._

import org.apache.spark.sql.Dataset

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

//import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//import org.princeton.billmatch.feature._


object BucketedRandomProjectionLSHExample {

  def doWorkForPair[T <: LSHModel[T]](
      model: T,
      pp: List[Long],
      datasetAB: Dataset[_],
      threshold: Double
      ): Unit = {
    val state_pair = pp(0).toString+pp(1).toString

    val dfA = datasetAB.filter(col("state") === pp(0))
    val dfB = datasetAB.filter(col("state") === pp(1))

    val transformedA = model.transform(dfA).cache()
    val transformedB = model.transform(dfB).cache()

    // Approximate similarity join
    //val result1 = model.approxSimilarityJoin(dfA, dfB, 15).select(col("datasetA.primary_key").alias("pk1"),col("datasetB.primary_key").alias("pk2"),col("distCol")).cache()
    //result1.printSchema()
    //result1.show()
 
    model.approxSimilarityJoin(transformedA, transformedB, threshold).select(col("datasetA.primary_key").alias("pk1"),col("datasetB.primary_key").alias("pk2"),col("distCol")).write.parquet("/user/alexeys/test_similarity_join"+state_pair)
  }

  def main(args: Array[String]) {

    //Test with actual text data
    val spark = SparkSession.builder().appName("MinHashExample")
      //.config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.6")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "10g")
      .getOrCreate()
 
    import spark.implicits._

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("makeCartesian")

    //Test with actual text data
    def compactSelector_udf = udf((s: String) => {

       val probe = s.toLowerCase()
       val compactPattern = "compact".r
       val isCompact = compactPattern.findFirstIn(probe).getOrElse("")

       val uniformPattern = "uniform".r
       val isUniform = uniformPattern.findFirstIn(probe).getOrElse("")

       (isCompact.isEmpty() && isUniform.isEmpty())
    })

    val vv: String = params.getString("makeCartesian.docVersion") //like "Enacted"
     val input = spark.read.json(params.getString("makeCartesian.inputFile")).filter($"docversion" === vv).filter(compactSelector_udf(col("content")))

    //val npartitions = (4*input.count()/1000).toInt
    //val bills = input.coalesce(100).cache() - 18573 seconds, vs 30000 seconds sequential
    val bills = input.repartition(400,col("primary_key")).cache()  //Math.max(npartitions,200),col("primary_key")) //,col("content"))
    bills.explain

    val nGramGranularity = params.getInt("makeCartesian.nGramGranularity")
    val addNGramFeatures = params.getBoolean("makeCartesian.addNGramFeatures")
    val numTextFeatures = params.getInt("makeCartesian.numTextFeatures")

    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))) //.drop("content")

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    val tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val prefeaturized_df = remover.transform(tokenized_df).select(col("primary_key"),col("content"),col("docversion"),col("docid"),col("state"),col("year"),col("filtered"))

    val ngram = new NGram().setN(nGramGranularity).setInputCol("filtered").setOutputCol("ngram")
    val ngram_df = ngram.transform(prefeaturized_df)
 
    //hashing
    val hashingTF = new HashingTF().setInputCol("ngram").setOutputCol("keys").setNumFeatures(numTextFeatures)
    val featurized_df = hashingTF.transform(ngram_df).select("keys","primary_key","state")

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(featurized_df)

    //get distinct states
    val states = featurized_df.select("state").distinct().as[Long].rdd.collect().toList.combinations(2).toList.par
    val state_pairs_results = states.foreach(pair => doWorkForPair(model,pair,featurized_df,15))

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
 
    spark.stop()
  }

}
