import scopt.OptionParser

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

  case class Params(inputBillsFile: String = null, inputPairsFile: String = null, outputMainFile: String = null, outputFilteredFile: String = null, measureName: String = null, nPartitions: Int = 0, numTextFeatures: Int = 10)
    extends AbstractParams[Params]

  /*
    Experimental
  */
  def converted(row: scala.collection.Seq[Any]) : Tuple2[Long,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[Long]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    val t0 = System.nanoTime()

    val defaultParams = Params()

    val parser = new OptionParser[Params]("AdhocAnalyzer") {
      head("AdhocAnalyzer: an app. that performs document or section similarity searches starting off CartesianPairs")
      opt[Int]("nPartitions")
        .required()
        .text(s"Number of partitions in bills_meta RDD")
        .action((x, c) => c.copy(nPartitions = x))
      opt[Int]("numTextFeatures")
        .required()
        .text(s"Number of text features to keep in hashingTF")
        .action((x, c) => c.copy(numTextFeatures = x))
      opt[String]("measureName")
        .required()
        .text(s"Distance measure used")
        .action((x, c) => c.copy(measureName = x))
      opt[String]("inputBillsFile")
        .required()
        .text(s"Bill input file, one JSON per line")
        .action((x, c) => c.copy(inputBillsFile = x))
      opt[String]("inputPairsFile")
        .required()
        .text(s"CartesianPairs object input file")
        .action((x, c) => c.copy(inputPairsFile = x))
      opt[String]("outputMainFile")
        .required()
        .text(s"outputMainFile: key-key pairs and corresponding similarities, as Tuple2[Tuple2[Long,Long],Double]")
        .action((x, c) => c.copy(outputMainFile = x))
      opt[String]("outputFilteredFile")
        .required()
        .text(s"outputFilteredFile: CartesianPairs passing similarity threshold")
        .action((x, c) => c.copy(outputFilteredFile = x))
      note(
        """
          |For example, the following command runs this app on a dataset:
          |
          | spark-submit  --class AdhocAnalyzer \
          | --master yarn-client --num-executors 30 --executor-cores 3 --executor-memory 10g \
          | target/scala-2.10/BillAnalysis-assembly-1.0.jar \
          | --docVersion Enacted --nPartitions 30 --inputBillsFile /scratch/network/alexeys/bills/lexs/bills_3.json --inputPairsFile /user/alexeys/valid_pairs --outputMainFile /user/alexeys/test_main_output --outputFilteredFile /user/alexeys/test_filtered_output
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName("AdhocAnalyzer")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")

    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    val bills = sqlContext.read.json(params.inputBillsFile)
    bills.repartition(col("primary_key"))
    bills.explain
    //bills.printSchema()
    //bills.show() 

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("content").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(bills)
    //tokenized_df.show(5,false)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val filtered_df = remover.transform(tokenized_df).drop("words")
    //filtered_df.printSchema()
    //filtered_df.show()

    //ngram = NGram(n=2, inputCol="filtered", outputCol="ngram")
    //ngram_df = ngram.transform(tokenized_df)

    //hashing
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(params.numTextFeatures)
    val featurized_df = hashingTF.transform(filtered_df).drop("filtered")
    //featurized_df.show(15,false)

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("pre_features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")
    rescaled_df.show(5) //,false)

    val hashed_bills = featurized_df.select("primary_key","rawFeatures").rdd.map(row => converted(row.toSeq))
    //println(hashed_bills.collect())

    //Experimental
    //First, run the hashing step here
    println(params.inputPairsFile)
    val cartesian_pairs = spark.objectFile[CartesianPair](params.inputPairsFile).map(pp => (pp.pk1,pp.pk2))

    var distanceMeasure: DistanceMeasure = null
    var threshold: Double = 0.0

    params.measureName match {
      case "cosine" => {
        distanceMeasure = CosineDistance
        threshold = ???
      }
      case "hamming" => {
        distanceMeasure = HammingDistance
        threshold = ???
      }
      case "euclidean" => {
        distanceMeasure = EuclideanDistance
        threshold = ???
      }
      case "manhattan" => {
        distanceMeasure = ManhattanDistance
        threshold = ???
      }
      case "jaccard" => {
        distanceMeasure = JaccardDistance
        threshold = ???
      }
      case other: Any =>
        throw new IllegalArgumentException(
          s"Only hamming, cosine, euclidean, manhattan, and jaccard distances are supported but got $other."
        )
    }

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    //println(firstjoin.count())
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues({case (v1,v2) => distanceMeasure.compute(v1.toSparse,v2.toSparse)})
    //matches.collect().foreach(println)
    matches.saveAsObjectFile(params.outputMainFile)

    //scala.Tuple2[Long, Long]
    //Experimental
    matches.filter(kv => (kv._2 > threshold)).keys.saveAsObjectFile(params.outputFilteredFile)
 
    spark.stop()
   }
}
