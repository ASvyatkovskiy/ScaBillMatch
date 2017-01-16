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


object MinHashLSHExample {

  /**
   * Compute the precision and recall of approximate similarity join
   * @param lsh The lsh instance
   * @param datasetA One of the datasets to join
   * @param datasetB Another dataset to join
   * @param threshold The threshold for the distance of record pairs
   * @tparam T The class type of lsh
   * @return A tuple of two doubles, representing precision and recall rate
   */
  def calculateApproxSimilarityJoin[T <: LSHModel[T]](
      lsh: LSH[T],
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double): (Double, Double) = {
    val model = lsh.fit(datasetA)
    val inputCol = model.getInputCol

    // Compute expected
    val distUDF = udf((x: Vector, y: Vector) => model.keyDistance(x, y), DataTypes.DoubleType)
    val expected = datasetA.as("a").crossJoin(datasetB.as("b"))
      .filter(distUDF(col(s"a.$inputCol"), col(s"b.$inputCol")) < threshold)

    // Compute actual
    val actual = model.approxSimilarityJoin(datasetA, datasetB, threshold)

    // Compute precision and recall
    val correctCount = actual.filter(col("distCol") < threshold).count().toDouble
    (correctCount / actual.count(), correctCount / expected.count())
  }


  def calculateApproxSimilarityJoin2[T <: LSHModel[T]](
      lsh: LSH[T],
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double,
      state_pair: String): Unit = {
    val model = lsh.fit(datasetA)
    val inputCol = model.getInputCol

    println("Compute actual")
    val actual = model.approxSimilarityJoin(datasetA, datasetB, threshold)
    //actual.show(4000)
    actual.printSchema()
    actual.select(col("datasetA.primary_key").alias("pk1"),col("datasetB.primary_key").alias("pk2"),col("distCol")).write.parquet("/user/alexeys/test_similarity_join"+state_pair)
    //actual.select(col("datasetA.primary_key").alias("pk1"),col("datasetB.primary_key").alias("pk2"),col("distCol")).show() 
  }

  def calculateFor2States[T <: LSHModel[T]](
      lsh: LSH[T],
      pp: List[Long],
      df: Dataset[_],
      threshold: Double): Unit = {

      val part1 = df.filter(col("state") === pp(0)) //.coalesce(200).cache()
      val part2 = df.filter(col("state") === pp(1))
      val state_pair = pp(0).toString+pp(1).toString
      calculateApproxSimilarityJoin2(lsh,part1,part2,threshold,state_pair)
  }

  def calculateFor2States2[T <: LSHModel[T]](
      lsh: LSH[T],
      pp: Tuple2[Int,List[Int]],
      df: Dataset[_],
      threshold: Double): Unit = {

      val part1 = df.filter(col("state") === pp._1) //.coalesce(200).cache()
      for (p <- pp._2) {
        val part2 = df.filter(col("state") === p) 
        val state_pair = pp._1.toString+p.toString
        calculateApproxSimilarityJoin2(lsh,part1,part2,threshold,state_pair)
      } 
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

    /*
    //Artificial toy data
    val data1 = {
      for (i <- 0 until 20) yield Vectors.sparse(100, (5 * i until 5 * i + 5).map((_, 1.0)))
    }
    val df1 = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("keys")

    val data2 = {
      for (i <- 0 until 30) yield Vectors.sparse(100, (3 * i until 3 * i + 3).map((_, 1.0)))
    }
    val df2 = spark.createDataFrame(data2.map(Tuple1.apply)).toDF("keys")
    */

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

    val mh = new MinHashLSH().setNumHashTables(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(12345)

    //get distinct states
    //val states = Map((39,List(8,21)),(8,List(21))).par //featurized_df.select("state").distinct().as[Long].rdd.collect().toList.combinations(2).toList.par
    val states = featurized_df.select("state").distinct().as[Long].rdd.collect().toList.combinations(2).toList.par
    val state_pairs_results = states.foreach(pair => calculateFor2States(mh,pair,featurized_df,0.5))

    //calculateApproxSimilarityJoins2(mh,states,featurized_df,0.6)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
 
    spark.stop()
  }

}
