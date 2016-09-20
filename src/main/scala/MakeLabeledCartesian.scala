/*
Application: MakeLabeledCartesian, produce all the pairs of primary keys of the documents satisfying a predicate.

Following parameters need to be filled in the resources/makeCartesian.conf file:
	docVersion: document version: consider document pairs having a specific version. E.g. Introduced, Enacted...
	nPartitions: number of partitions in bills_meta RDD
        onlyInOut: a switch between in-out of state and using both in-out and in-in state pairs
	use_strict: boolean, yes or no to consider strict parameters
	strict_state: specify state (a long integer from 1 to 50) for one-against-all user selection (strict)
	strict_docid: specify document ID for one-against-all user selection (strict)
	strict_year: specify year for one-against-all user selection (strict)
	inputFile: input file, one JSON per line
	outputFile: output file

Example to explore output in spark-shell:
$ spark-shell --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar 
scala> val mydata = sc.objectFile[CartesianPair]("/user/alexeys/valid_pairs")
mydata: org.apache.spark.rdd.RDD[CartesianPair] = MapPartitionsRDD[3] at objectFile at <console>:27

scala> mydata.take(5)
res1: Array[CartesianPair] = Array()
*/
import com.typesafe.config._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._

//import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

import java.io._

import org.apache.spark.ml.clustering.{KMeans, BisectingKMeans}


object MakeLabeledCartesian {

  def pairup (document: MetaLabeledDocument, thewholething: org.apache.spark.broadcast.Broadcast[Array[MetaLabeledDocument]], strict_params: Tuple4[Boolean, Int, java.lang.String, Int], onlyInOut: Boolean) : (MetaLabeledDocument, Array[CartesianPair]) = {

    val documents = thewholething.value

    val (use_strict,strict_state,strict_docid,strict_year) = strict_params

    val idocversion = document.docversion
    val istate = document.state
    val iyear = document.year
    val idocid = document.docid
    val pk1 = document.primary_key
    val label1 = document.prediction

    //var output_premap: Tuple2(MetaLabeledDocument, Array[CartesianPair]) = Tuple2()
    var output_arr: ArrayBuffer[CartesianPair] = new ArrayBuffer[CartesianPair]()

    for (jevent <- documents) {
       val jdocversion = jevent.docversion
       val jstate = jevent.state
       val jyear = jevent.year
       val pk2 = jevent.primary_key
       val label2 = jevent.prediction
       if (use_strict) {
         //extra condition
         if (istate == strict_state && idocid == strict_docid && iyear == strict_year) {
           if (pk1 < pk2 && label1 == label2 && istate != jstate) {
              var output: CartesianPair = CartesianPair(pk1,pk2)
              output_arr += output
           }
         } 
       } else {
          //simple condition
          if (onlyInOut) {
             if (pk1 < pk2 && label1 == label2 && istate != jstate) {
                var output: CartesianPair = CartesianPair(pk1,pk2)
                output_arr += output
             }
           } else {
             //in-out and in-in
             if (pk1 < pk2 && label1 == label2) {
                var output: CartesianPair = CartesianPair(pk1,pk2)
                output_arr += output
             }
           } 
        }
     }
     (document,output_arr.toArray)
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

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
      (7*len/350000000).toInt
  }

  def main(args: Array[String]) {

    println(s"\nExample submit command: spark-submit --class MakeLabeledCartesian --master yarn --queue production --num-executors 30 --executor-cores 3 --executor-memory 10g target/scala-2.11/BillAnalysis-assembly-2.0.jar\n")

    val t0 = System.nanoTime()

    val params = ConfigFactory.load("makeCartesian")

    run(params)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }

  def run(params: Config) {

    val spark = SparkSession.builder().appName("MakeLabeledCartesian")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    val vv: String = params.getString("makeCartesian.docVersion") //like "Enacted"
    val input = spark.read.json(params.getString("makeCartesian.inputFile")).filter($"docversion" === vv)
    input.printSchema()
    input.show()

    val npartitions = (4*input.count()/1000).toInt

    val bills = input.repartition(Math.max(npartitions,200),col("primary_key"),col("content"))
    bills.explain

    //sqlContext.udf.register("cleaner_udf", (s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content")))  //.drop("content")

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    var prefeaturized_df = remover.transform(tokenized_df).drop("words")

    //hashing
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(params.getInt("makeCartesian.numTextFeatures"))
    val featurized_df = hashingTF.transform(prefeaturized_df)

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    val rescaled_df = idfModel.transform(featurized_df).drop("rawFeatures")

    // Trains a k-means model
    // setDefault(
    //k -> 2,
    //maxIter -> 20,
    //initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    //initSteps -> 5,
    //tol -> 1e-4)
    val kval: Int = 150
    val kmeans = new KMeans().setK(kval).setMaxIter(40).setFeaturesCol("features").setPredictionCol("prediction")
    //val kmeans = new BisectingKMeans().setK(kval).setSeed(1).setMaxIter(40).setFeaturesCol("features").setPredictionCol("prediction")
    val model = kmeans.fit(rescaled_df)

    var clusters_df = model.transform(rescaled_df)
    //Setup for splitting by cluster on the step2
    clusters_df.select("primary_key","docversion","docid","state","year","prediction","content").write.parquet(params.getString("makeCartesian.outputParquetFile"))

    val WSSSE = model.computeCost(rescaled_df)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    //model.explainParams()

    val explained = model.extractParamMap()
    println(explained)

    var bills_meta = clusters_df.select("primary_key","docversion","docid","state","year","prediction").as[MetaLabeledDocument]
    bills_meta.printSchema()
    //bills_meta.write.parquet("/user/alexeys/kMeans_test")

    var bills_meta_bcast = spark.sparkContext.broadcast(bills_meta.collect())

    val strict_params = (params.getBoolean("makeCartesian.use_strict"),params.getInt("makeCartesian.strict_state"),params.getString("makeCartesian.strict_docid"),params.getInt("makeCartesian.strict_year"))

    //will be array of tuples, but the keys are unique
    var cartesian_pairs = bills_meta.rdd.repartition(params.getInt("makeCartesian.nPartitions"))
                          .map(x => pairup(x,bills_meta_bcast, strict_params, params.getBoolean("makeCartesian.onlyInOut")))
                          .filter({case (dd,ll) => (ll.length > 0)})
                          .map({case(k,v) => v}).flatMap(x => x) //.groupByKey()    
    cartesian_pairs.saveAsObjectFile(params.getString("makeCartesian.outputFile"))

    spark.stop()
   }
}

case class MetaLabeledDocument(primary_key: String, prediction: Long, state: Long, docid: String, docversion: String, year: Long)
case class CartesianPair(pk1: String, pk2: String)
