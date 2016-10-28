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

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.StopWordsRemover

//import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

import java.io._

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors


object LDAAnalyzer {

  def converted(row: scala.collection.Seq[Any]) : Tuple2[String,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[String]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit  --class LDAAnalyzer --master yarn-client --num-executors 40 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("ldaAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }


  def customNPartitions(directory: File) : Int = {
      var len = 0.0
      val all: Array[File] = directory.listFiles()
      for (f <- all) {
        if (f.isFile())
            len = len + f.length()
        else
            len = len + customNPartitions(f)
      }
      //353 GB worked with 7000 partitions
      val npartitions = (7.0*len/350000000.0).toInt
      npartitions  
  }

  def appendFeature(a: WrappedArray[String], b: WrappedArray[String]) : WrappedArray[String] = {
     a ++ b
  }   

  def run(params: Config) {

    val spark = SparkSession.builder().appName("LDAAnalyzer")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    val vv: String = params.getString("ldaAnalyzer.docVersion") //like "Enacted"
    val input = spark.read.json(params.getString("ldaAnalyzer.inputBillsFile")).filter($"docversion" === vv)
    val npartitions = (4.0*input.count()/100000.0).toInt

    val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content")) //.filter("docversion == Introduced")
    bills.explain

    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))).drop("content")

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    var prefeaturized_df = remover.transform(tokenized_df).drop("words")

    prefeaturized_df = prefeaturized_df.select(col("primary_key"),col("filtered").alias("combined"))

    //hashing
    var hashingTF = new HashingTF().setInputCol("combined").setOutputCol("rawFeatures").setNumFeatures(params.getInt("ldaAnalyzer.numTextFeatures"))
    val featurized_df = hashingTF.transform(prefeaturized_df)

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")
    rescaled_df.printSchema()


    // Trains LDA model
    val kval: Int = 100
    val lda = new LDA().setK(kval).setMaxIter(10)

    val model = lda.fit(rescaled_df)

    val ll = model.logLikelihood(rescaled_df)
    val lp = model.logPerplexity(rescaled_df)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")


    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show()

    val clusters_df = model.transform(rescaled_df)
    clusters_df.show()
    clusters_df.printSchema()

    //FIXME save the dataframe with predicted labels if you need
    //clusters_df.select("primary_key","prediction").write.format("parquet").save(params.getString("ldaAnalyzer.outputFile"))

    spark.stop()
   }
}
