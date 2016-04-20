import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

object AdhocBillAnalysis {

  /*
  def appendFeature(sv: SparseVector, adhoc_feature1: Int, adhoc_feature2: Int) : Vector = {

      //case sv: SparseVector =>
      val inputValues = sv.values
      val inputIndices = sv.indices
      val inputValuesLength = inputValues.length
      val dim = sv.size

      var adhoc_features = Array(adhoc_feature1,adhoc_feature2)
      val addhoc_size = adhoc_features.length

      val outputValues = Array.ofDim[Double](inputValuesLength + addhoc_size)
      val outputIndices = Array.ofDim[Int](inputValuesLength + addhoc_size)

      System.arraycopy(inputValues, 0, outputValues, 0, inputValuesLength)
      System.arraycopy(inputIndices, 0, outputIndices, 0, inputValuesLength)

      for (i <- 1 to addhoc_size) {
        outputValues(inputValuesLength-1+i) = adhoc_features(i-1).toDouble
      }
      outputIndices(inputValuesLength) = dim

      Vectors.sparse(dim + addhoc_size, outputIndices, outputValues)
      //case _ => throw new IllegalArgumentException(s"Do not support vector type ${vector.getClass}")
       
  }
  */

  /* 
  def defineWeights(label: WrappedArray[Int]) : Double = {
      var weight: Double = 0.0
      //upscale under represented class
      if (label == 1.0) weight = 10.0 
      weight
  }
  */

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("KaggleDato")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    val bills = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_3.json")
    bills.repartition(col("primary_key"))
    bills.explain
    bills.printSchema()
    bills.show() 

    //register UDFs
    //sqlContext.udf.register("countfeat", (s: WrappedArray[String]) => s.length)
    //def countfeat_udf = udf((s: WrappedArray[String]) => s.length)

    //create udf that returns length of the list, 
    //val links_cnt_df = bills.withColumn("links_cnt",countfeat_udf(col("links")))
    //links_cnt_df.printSchema()
    //links_cnt_df.show()

    //image cnt features
    //val images_cnt_df = links_cnt_df.withColumn("images_cnt",countfeat_udf(col("images")))
    //images_cnt_df.printSchema()
    //images_cnt_df.show()

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("content").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(bills)
    //tokenized_df.show()

    //remove stopwords 
    //var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    //val filtered_df = remover.transform(tokenized_df).drop("words")
    //filtered_df.printSchema()
    //filtered_df.show()

    //FIXME still need to try to squeeze ngrams in 
    var ngram = new NGram().setN(5).setInputCol("words").setOutputCol("ngram")
    val ngram_df = ngram.transform(tokenized_df)

    //hashing
    var hashingTF = new HashingTF().setInputCol("ngram").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurized_df = hashingTF.transform(ngram_df)

    //idf weighting
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("pre_features")
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")
    rescaled_df.printSchema()
    rescaled_df.show()

    //make adhoc DF
    //sqlContext.udf.register("addhocfeat_appender", appendFeature _)
    //def appendFeature_udf = udf(appendFeature _)
    //var adhoc_df = rescaled_df.withColumn("features", appendFeature_udf(col("pre_features"),col("links_cnt"),col("images_cnt"))) 
    //adhoc_df.printSchema()
    //adhoc_df.show()

   }
}
