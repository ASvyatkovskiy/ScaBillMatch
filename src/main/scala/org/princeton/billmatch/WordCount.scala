package org.princeton.billmatch

//import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover, CountVectorizer, CountVectorizerModel}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.princeton.billmatch.feature._

object WordCount { 

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

    val vv: String = "Introduced"
    val inputFile: String = "file:///scratch/network/alexeys/bills/lexs/bills_combined_wu_50_p*"
    val input = spark.read.json(inputFile).filter($"docversion" === vv).filter(Utils.lengthSelector_udf(col("content")))
    //val input = spark.read.json(inputFile).filter(Utils.lengthSelector_udf(col("content")))

    val bills = input.repartition(400,col("primary_key")).cache()  //Math.max(npartitions,200),col("primary_key")) 
    bills.explain

    val nGramGranularity = 5

    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    def cleaner_udf2 = udf((s: String) => s.replaceAll("\\d",""))
    def cleaner_udf3 = udf((s: String) => s.replaceAll("(\\s[a-zA-Z]{1}\\s|\\s[a-zA-Z]{1}\\n|^[a-zA-Z]{1}\\s)",""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))).withColumn("cleaned",cleaner_udf2(col("cleaned"))).withColumn("cleaned",cleaner_udf3(col("cleaned")))

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    val tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val prefeaturized_df = remover.transform(tokenized_df).select(col("primary_key"),col("content"),col("docversion"),col("docid"),col("state"),col("year"),col("filtered"))

    val ngram = new NGram().setN(nGramGranularity).setInputCol("filtered").setOutputCol("ngram")
    val ngram_df = ngram.transform(prefeaturized_df)

    val result = ngram_df.groupBy("ngram").agg(count("docid").alias("result")).select("result")
    result.write.parquet("/user/alexeys/all_5grams")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
  }

}
