//import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover}
import org.apache.spark.ml.clustering.{KMeans, BisectingKMeans}

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,CoordinateMatrix}

//we have to deal with this nonsense for now
import org.apache.spark.mllib.linalg.{
  Vector => OldVector,
  Vectors => OldVectors,
  SparseVector => OldSparseVector,
  DenseVector => OldDenseVector,
  VectorUDT => OldVectorUDT}

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.linalg.{
   Vector => NewVector,
   Vectors => NewVectors,
   DenseVector => NewDenseVector,
   SparseVector => NewSparseVector
}

import java.io._

object Utils {

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalMatrix(mat: RowMatrix, diag: OldVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map(vec => {
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      OldVectors.dense(newArr)
    }))
  }

  /*MLlib ML convertion tools*/
  def toOld(v: NewVector): OldVector = v match {
    case sv: NewSparseVector => OldVectors.sparse(sv.size, sv.indices, sv.values)
    case dv: NewDenseVector => OldVectors.dense(dv.values)
  }

  def toNew(v: OldVector): NewVector = v match {
    case sv: OldSparseVector => NewVectors.sparse(sv.size, sv.indices, sv.values)
    case dv: OldDenseVector => NewVectors.dense(dv.values)
  }

  def pairup (document: MetaLabeledDocument, thewholething: org.apache.spark.broadcast.Broadcast[Array[MetaLabeledDocument]], strict_params: Tuple4[Boolean, Int, java.lang.String, Int], onlyInOut: Boolean) : (MetaLabeledDocument, Array[CartesianPair]) = {

    val documents = thewholething.value

    val (use_strict,strict_state,strict_docid,strict_year) = strict_params

    val idocversion = document.docversion
    val istate = document.state
    val iyear = document.year
    val idocid = document.docid
    val pk1 = document.primary_key
    val label1 = document.prediction

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

  //Calculate submatrix containing nColsToKeep of inital matrix A. This is used for truncating the V.T matrix in SVD 
  def truncatedMatrix(a: Matrix, nColsToKeep: Int): Matrix = {
      Matrices.dense(a.numRows,nColsToKeep,a.toArray.slice(0,a.numRows*nColsToKeep))
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
      (7*len/350000000).toInt
   }

   def DIMSUMSuite(mat: RowMatrix, threshold: Double) : CoordinateMatrix = { 
     //Compute similar columns with estimation using DIMSUM
     val approx = mat.columnSimilarities(threshold)
     approx
   }

   def KMeansSuite(rescaled_df: DataFrame, kval: Int) : DataFrame = {
      // Trains a k-means model
      // setDefault(
      //k -> 2,
      //maxIter -> 20,
      //initMode -> MLlibKMeans.K_MEANS_PARALLEL,
      //initSteps -> 5,
      //tol -> 1e-4)
      val kmeans = new KMeans().setK(kval).setMaxIter(40).setFeaturesCol("features").setPredictionCol("prediction")
      //val kmeans = new BisectingKMeans().setK(params.getInt("makeCartesian.kval")).setSeed(1).setMaxIter(40).setFeaturesCol("features").setPredictionCol("prediction")

      val model = kmeans.fit(rescaled_df)
      var clusters_df = model.transform(rescaled_df)

      val WSSSE = model.computeCost(rescaled_df)
      println("Within Set Sum of Squared Errors = " + WSSSE)
      model.explainParams()
      val explained = model.extractParamMap()
      println(explained)
      clusters_df
  } 

  def LSA(spark: SparkSession, dataRDD: RDD[OldVector], numConcepts: Int, keepConcepts: Int) : DataFrame = {

    val mat: RowMatrix = new RowMatrix(dataRDD) //that assumes RDD of Vectors
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(numConcepts, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: OldVector = svd.s  // The singular values are stored in a local dense vector.
    var VT: Matrix = svd.V.transpose  // The V factor is a local dense matrix.
    val us: RowMatrix = multiplyByDiagonalMatrix(U,s)
    var reconstructed = us.multiply(truncatedMatrix(VT,keepConcepts)).rows.map(x => toNew(x)).map(x => Row(x))

    val reco_schema = StructType(Seq(StructField("features", VectorType, false)))
    spark.createDataFrame(reconstructed,reco_schema)
  }


  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
     val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
       .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
       .groupByKey
       .sortByKey().map(_._2) // sort rows and remove row indexes
       .map(buildRow) // restore order of elements in each row and remove column indexes
     new RowMatrix(transposedRowsRDD)
   }


  def rowToTransposedTriplet(row: OldVector, rowIndex: Long): Array[(Long, (Long, Double))] = {
     val indexedRow = row.toArray.zipWithIndex
     indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
   }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): OldVector = {
     val resArr = new Array[Double](rowWithIndexes.size)
     rowWithIndexes.foreach{case (index, value) =>
         resArr(index.toInt) = value
     }
     OldVectors.dense(resArr)
   } 


  def LSAmatrix(spark: SparkSession, dataRDD: RDD[OldVector], numConcepts: Int, keepConcepts: Int) : RowMatrix = {
    //same as above, but prepare output in the format DIMSUM expects it (transposed RowMatrix)
    val mat: RowMatrix = new RowMatrix(dataRDD) //that assumes RDD of Vectors
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(numConcepts, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: OldVector = svd.s  // The singular values are stored in a local dense vector.
    var VT: Matrix = svd.V.transpose  // The V factor is a local dense matrix.
    val us: RowMatrix = multiplyByDiagonalMatrix(U,s)
    val reconstructed = us.multiply(truncatedMatrix(VT,keepConcepts))
    transposeRowMatrix(reconstructed)
  }

  def appendFeature(a: WrappedArray[String], b: WrappedArray[String]) : WrappedArray[String] = {
     a ++ b
  }

  def cleaner_udf = udf((s: String) => s.replaceAll("(\\d|,|:|;|\\?|!)", ""))

  def appendFeature_udf = udf(appendFeature _)

  def extractFeatures(bills: DataFrame, numTextFeatures: Int, addNGramFeatures: Boolean, nGramGranularity: Int) : DataFrame = {
    val cleaned_df = bills.withColumn("cleaned",cleaner_udf(col("content"))) //.drop("content")

    //tokenizer = Tokenizer(inputCol="text", outputCol="words")
    var tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
    val tokenized_df = tokenizer.transform(cleaned_df)

    //remove stopwords 
    var remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    var prefeaturized_df = remover.transform(tokenized_df).drop("words")

    if (addNGramFeatures) {

       val ngram = new NGram().setN(nGramGranularity).setInputCol("filtered").setOutputCol("ngram")
       val ngram_df = ngram.transform(prefeaturized_df)

       //prefeaturized_df = ngram_df.withColumn("combined", appendFeature_udf(col("filtered"),col("ngram"))).drop("filtered").drop("ngram").drop("cleaned")
       prefeaturized_df = ngram_df.select(col("primary_key"),col("content"),col("docversion"),col("docid"),col("state"),col("year"),col("ngram").alias("combined"))
    } else {
       prefeaturized_df = prefeaturized_df.select(col("primary_key"),col("content"),col("docversion"),col("docid"),col("state"),col("year"),col("filtered").alias("combined"))
       prefeaturized_df.printSchema()
    }

    //hashing
    var hashingTF = new HashingTF().setInputCol("combined").setOutputCol("rawFeatures").setNumFeatures(numTextFeatures)
    val featurized_df = hashingTF.transform(prefeaturized_df).drop("combined")

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //val Array(train, cv) = featurized_df.randomSplit(Array(0.7, 0.3))
    var idfModel = idf.fit(featurized_df)
    idfModel.transform(featurized_df).drop("rawFeatures").drop("content")
  }

  def convertedr(row: scala.collection.Seq[Any]) : (Int,NewVector) = {
    val ret = row.asInstanceOf[WrappedArray[Any]]
    val first = ret(0).asInstanceOf[Int]
    val second = ret(1).asInstanceOf[NewVector]
    (first,second)
  }
}
