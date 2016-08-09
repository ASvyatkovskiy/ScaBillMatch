/*BillAnalyzer: an app. that performs document or section similarity searches starting off CartesianPairs

Following parameters need to be filled in the resources/billAnalyzer.conf file:
    numTextFeatures: Number of text features to keep in hashingTF
    addNGramFeatures: Boolean flag to indicate whether to add n-gram features
    nGramGranularity: granularity of a rolling n-gram
    measureName: Similarity measure used
    inputBillsFile: Bill input file, one JSON per line
    inputPairsFile: CartesianPairs object input file
    outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[String,String],Double]
    outputFilteredFile: CartesianPairs passing similarity threshold
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
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

object BillAnalyzer {

  def converted(row: scala.collection.Seq[Any]) : Tuple2[String,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[String]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit  --class BillAnalyzer --master yarn-client --queue production --num-executors 40 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("billAnalyzer")
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
      (7.0*len/350000000.0).toInt
  }

  def appendFeature(a: WrappedArray[String], b: WrappedArray[String]) : WrappedArray[String] = {
     a ++ b
  }   

  def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))

  def appendFeature_udf = udf(appendFeature _)

  def run(params: Config) {

    val spark = SparkSession
      .builder()
      .appName("MakeLabeledCartesian")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._
 
    val vv: String = params.getString("billAnalyzer.docVersion") //like "Enacted"

    val jsonSchema = spark.read.json(spark.sparkContext.parallelize(Array("""{"docversion": "str", "docid": "str", "primary_key": "str", "content": "str"}""")))
    val input = spark.read.format("json").schema(jsonSchema.schema).load(params.getString("billAnalyzer.inputBillsFile")).filter($"docversion" === vv)
    val npartitions = (4*(input.count()/1000)).toInt

    val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content"))
    bills.explain

    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))).drop("content")

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    var prefeaturized_df = remover.transform(tokenized_df).drop("words")

    if (params.getBoolean("billAnalyzer.addNGramFeatures")) {

       val ngram = new NGram().setN(params.getInt("billAnalyzer.nGramGranularity")).setInputCol("filtered").setOutputCol("ngram")
       val ngram_df = ngram.transform(prefeaturized_df)

       prefeaturized_df = ngram_df.withColumn("combined", appendFeature_udf(col("filtered"),col("ngram"))).drop("filtered").drop("ngram").drop("cleaned")
    } else {
       prefeaturized_df = prefeaturized_df.select(col("primary_key"),col("filtered").alias("combined"))
       prefeaturized_df.printSchema()
    }

    //hashing
    var hashingTF = new HashingTF().setInputCol("combined").setOutputCol("rawFeatures").setNumFeatures(params.getInt("billAnalyzer.numTextFeatures"))
    val featurized_df = hashingTF.transform(prefeaturized_df)

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("pre_features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")

    val hashed_bills = rescaled_df.select("primary_key","pre_features").rdd.map(row => converted(row.toSeq))

    //First, run the hashing step here
    val nPartJoin = 2*customNPartitions(new File(params.getString("billAnalyzer.inputPairsFile")))
    println("Running join with "+nPartJoin+" partitions")
    val cartesian_pairs = spark.sparkContext.objectFile[CartesianPair](params.getString("billAnalyzer.inputPairsFile"),Math.max(200,nPartJoin)).map(pp => (pp.pk1,pp.pk2))

    var similarityMeasure: SimilarityMeasure = null
    var threshold: Double = 0.0

    params.getString("billAnalyzer.measureName") match {
      case "cosine" => {
        similarityMeasure = CosineSimilarity
        //threshold = ???
      }
      case "hamming" => {
        similarityMeasure = HammingSimilarity
        //threshold = ???
      }
      case "manhattan" => {
        similarityMeasure = ManhattanSimilarity
        //threshold = ???
      }
      case "jaccard" => {
        similarityMeasure = JaccardSimilarity
        //threshold = ???
      }
      case other: Any =>
        throw new IllegalArgumentException(
          s"Only hamming, cosine, euclidean, manhattan, and jaccard similarities are supported but got $other."
        )
    }

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})

    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues({case (v1,v2) => similarityMeasure.compute(v1.toSparse,v2.toSparse)})
    
    matches.saveAsObjectFile(params.getString("billAnalyzer.outputMainFile"))

    spark.stop()
   }
}
