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

//import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

//import breeze.linalg.{DenseVector=>BDV, SparseVector=>BSV, Vector=>BV}

//import com.invincea.spark.hash.{LSH, LSHModel}
//import org.apache.spark.mllib.linalg.{Matrix, Matrices}

object AdhocAnalyzer {

  /*
    Experimental
  */
  def converted(row: scala.collection.Seq[Any]) : Tuple2[Long,SparseVector] = { 
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[Long]
    val second = ret(1).asInstanceOf[Vector]
    Tuple2(first,second.toSparse)
  }

  /*
    Experimental
  */
  def converted1(row: scala.collection.Seq[Any]) : SparseVector = {
    val ret = row.asInstanceOf[WrappedArray[Vector]]
    ret(0).toSparse
  }

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("AdhocAnalyzer")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")

    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)

    val bills = sqlContext.read.json("file:///scratch/network/alexeys/bills/lexs/bills_10.json")
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
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(10)
    val featurized_df = hashingTF.transform(filtered_df).drop("filtered")
    //featurized_df.show(5,false)

    //FIXME idf weighting. That gave me identical zeros??
    /*
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("pre_features")
    val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(train)
    val rescaled_df = idfModel.transform(cv).drop("rawFeatures")
    rescaled_df.show(5,false)
    */

    val hashed_bills = featurized_df.select("primary_key","rawFeatures").rdd.map(row => converted(row.toSeq))
    //val hashed_bills = featurized_df.select("rawFeatures").rdd.map(row => converted1(row.toSeq))

    //println(hashed_bills.collect())
    /*
      Experimental
    */
    //val lsh = new  LSH(data = hashed_bills, p = 65537, m = 1000, numRows = 1000, numBands = 25, minClusterSize = -1)
    //val model = lsh.run
    //model.scores.collect().foreach(println)
    //println(model.clusters.count())  

    //println(manOf(model))
    //println(manOf(model.scores.collect()))

    //Experimental
    //First, run the hashing step here
    val cartesian_pairs = spark.objectFile[CartesianPair]("/user/alexeys/test_object0/").map(pp => (pp.pk1,pp.pk2))

    val firstjoin = cartesian_pairs.map({case (k1,k2) => (k1, (k1,k2))})
        .join(hashed_bills)
        .map({case (_, ((k1, k2), v1)) => ((k1, k2), v1)})
    
    val matches = firstjoin.map({case ((k1,k2),v1) => (k2, ((k1,k2),v1))})
        .join(hashed_bills)
        .map({case(_, (((k1,k2), v1), v2))=>((k1, k2),(v1, v2))}).mapValues({case (v1,v2) => CosineDistance.compute(v1.toSparse,v2.toSparse)})
    //matches.collect().foreach(println)
    matches.saveAsObjectFile("/user/alexeys/test_main_output0")

    //scala.Tuple2[Long, Long]
    //matches.filter(kv => (kv._2 > 70.0)).keys.saveAsObjectFile("/user/alexeys/test_new_filtered_pairs0")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    spark.stop()
   }
}
