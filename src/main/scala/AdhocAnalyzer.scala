/*AdhocAnalyzer: an app. that performs document or section similarity searches starting off CartesianPairs

Following parameters need to be filled in the resources/adhocAnalyzer.conf file:
    nPartitions: Number of partitions in bills_meta RDD
    numTextFeatures: Number of text features to keep in hashingTF
    measureName: Distance measure used
    inputBillsFile: Bill input file, one JSON per line
    inputPairsFile: CartesianPairs object input file
    outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[String,String],Double]
    outputFilteredFile: CartesianPairs passing similarity threshold
*/

import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
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


object AdhocAnalyzer {

  /*
    Experimental
  */
  def converted(row: scala.collection.Seq[Any]) : Tuple2[String,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[String]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit  --class AdhocAnalyzer --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.10/BillAnalysis-assembly-1.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("adhocAnalyzer")
    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def run(params: Config) {

    val conf = new SparkConf().setAppName("AdhocAnalyzer")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")

    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext.implicits._
    

    val bills = sqlContext.read.json(params.getString("adhocAnalyzer.inputBillsFile"))
    bills.repartition(col("primary_key"))
    bills.show(5)

    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))).drop("content")
    cleaned_df.show(5)

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val filtered_df = remover.transform(tokenized_df).drop("words")

    //ngram = NGram(n=2, inputCol="filtered", outputCol="ngram")
    //ngram_df = ngram.transform(tokenized_df)

    //hashing
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures") //.setNumFeatures(params.getInt("adhocAnalyzer.numTextFeatures"))
    val featurized_df = hashingTF.transform(filtered_df).drop("filtered")

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("pre_features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")

    val hashed_bills = featurized_df.select("primary_key","rawFeatures").rdd.map(row => converted(row.toSeq))

    //Experimental
    //First, run the hashing step here
    val cartesian_pairs = spark.objectFile[CartesianPair](params.getString("adhocAnalyzer.inputPairsFile")).map(pp => (pp.pk1,pp.pk2))

    var distanceMeasure: DistanceMeasure = null
    var threshold: Double = 0.0

    params.getString("adhocAnalyzer.measureName") match {
      case "cosine" => {
        distanceMeasure = CosineDistance
        //threshold = ???
      }
      case "hamming" => {
        distanceMeasure = HammingDistance
        //threshold = ???
      }
      case "euclidean" => {
        distanceMeasure = EuclideanDistance
        //threshold = ???
      }
      case "manhattan" => {
        distanceMeasure = ManhattanDistance
        //threshold = ???
      }
      case "jaccard" => {
        distanceMeasure = JaccardDistance
        //threshold = ???
      }
      case other: Any =>
        throw new IllegalArgumentException(
          s"Only hamming, cosine, euclidean, manhattan, and jaccard distances are supported but got $other."
        )
    }

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})

    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues({case (v1,v2) => distanceMeasure.compute(v1.toSparse,v2.toSparse)})
    
    matches.saveAsObjectFile(params.getString("adhocAnalyzer.outputMainFile"))

    spark.stop()
   }
}
