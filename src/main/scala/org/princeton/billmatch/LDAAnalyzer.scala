package org.princeton.billmatch

/*LDAAnalyzer: an app. that performs document or section similarity searches starting off CartesianPairs

Following parameters need to be filled in the resources/ldaAnalyzer.conf file:
    numTextFeatures: Number of text features to keep in hashingTF
    addNGramFeatures: Boolean flag to indicate whether to add n-gram features
    nGramGranularity: granularity of a rolling n-gram
    inputBillsFile: Bill input file, one JSON per line
    outputMainFile: 
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.clustering.LDA

import scala.collection.mutable.WrappedArray

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

import org.princeton.billmatch.feature._

import java.io._
import scala.io.Source
import java.io.FileWriter

object LDAAnalyzer {

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit  --class LDAAnalyzer --master yarn-client --num-executors 40 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("ldaAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }


  def run(params: Config) {

    val spark = SparkSession.builder().appName("LDAAnalyzer")
      //.config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.shuffle.memoryFraction","0.7")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.driver.maxResultSize", "15g")
      .getOrCreate()

    import spark.implicits._

    lazy val vv: String = params.getString("ldaAnalyzer.docVersion") //like "Enacted"
    lazy val nGramGranularity = params.getInt("ldaAnalyzer.nGramGranularity")
    lazy val numTextFeatures = params.getInt("ldaAnalyzer.numTextFeatures")
    lazy val addNGramFeatures = params.getBoolean("ldaAnalyzer.addNGramFeatures")
    lazy val useStemming = params.getBoolean("ldaAnalyzer.useStemming")
    lazy val kval = params.getInt("ldaAnalyzer.kval")
    lazy val nmaxiter = params.getInt("ldaAnalyzer.nmaxiter")
    lazy val nOutTerms = params.getInt("ldaAnalyzer.nOutTerms")
    lazy val verbose = params.getBoolean("ldaAnalyzer.verbose")

    val input = spark.read.json(params.getString("ldaAnalyzer.inputFile")).filter($"docversion" === vv).filter(Utils.lengthSelector_udf(col("content")))
    val npartitions = (4*input.count()/1000).toInt
    val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content"))

    lazy val vocabLimit = params.getInt("ldaAnalyzer.vocabSizeLimit")
    var features_df = Utils.extractFeatures(bills,numTextFeatures,addNGramFeatures,nGramGranularity,true,useStemming,vocabLimit).cache()
    if (verbose) features_df.show
    // features_df.write.parquet(params.getString("ldaAnalyzer.outputFile")+"_features")    

    // Trains LDA model
    val lda = new LDA().setK(kval).setMaxIter(nmaxiter)

    val model = lda.fit(features_df)
  
    // if (verbose) {
    val ll = model.logLikelihood(features_df)
    val lp = model.logPerplexity(features_df)
    val fw: FileWriter = new FileWriter(params.getString("ldaAnalyzer.outputFile")+"_goodfit.dat")
    fw.write(s"$ll"+"\n")
    fw.write(s"$lp"+"\n")
    fw.close()
    // println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    // println(s"The upper bound bound on perplexity: $lp")
    // }

    // Describe topics.
    val topics = model.describeTopics(nOutTerms)
    if (verbose) {
      println("The topics described by their top-weighted terms:")
      topics.show(false)
      //val matrix = model.topicsMatrix
    }
    topics.write.parquet(params.getString("ldaAnalyzer.outputFile")+"_topics")

    val clusters_df = model.transform(features_df)
    if (verbose) {
      clusters_df.show(false)
      clusters_df.printSchema()
    }

    //save the dataframe with predicted labels if you need
    clusters_df.select("primary_key","topicDistribution").write.parquet(params.getString("ldaAnalyzer.outputFile"))

    spark.stop()
   }
}
