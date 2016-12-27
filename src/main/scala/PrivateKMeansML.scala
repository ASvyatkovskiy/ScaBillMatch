package org.apache.spark.ml.clustering

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{PrivateKMeans => MLlibPrivateKMeans, PrivateKMeansModel => MLlibPrivateKMeansModel}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Common params for PrivateKMeans and PrivateKMeansModelML
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol {

  /**
   * The number of clusters to create (k). Must be > 1. Note that it is possible for fewer than
   * k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
   * Default: 2.
   * @group param
   */
  @Since("1.5.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.5.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'k-means||'.",
    (value: String) => MLlibPrivateKMeans.validateInitMode(value))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 2 is almost always enough. Must be > 0. Default: 2.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initSteps = new IntParam(this, "initSteps", "The number of steps for k-means|| " +
    "initialization mode. Must be > 0.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitSteps: Int = $(initSteps)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

/**
 * :: Experimental ::
 * Model fitted by PrivateKMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.PrivateKMeans.
 */
@Since("1.5.0")
@Experimental
class PrivateKMeansModelML private[ml] (
    @Since("1.5.0") override val uid: String,
    private val parentModel: MLlibPrivateKMeansModel)
  extends Model[PrivateKMeansModelML] with KMeansParams with MLWritable {

  @Since("1.5.0")
  override def copy(extra: ParamMap): PrivateKMeansModelML = {
    val copied = copyValues(new PrivateKMeansModelML(uid, parentModel), extra)
    if (trainingSummary.isDefined) copied.setSummary(trainingSummary.get)
    copied.setParent(this.parent)
  }

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  @Since("2.0.0")
  def clusterCenters: Array[Vector] = parentModel.clusterCenters.map(_.asML)

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  // TODO: Replace the temp fix when we have proper evaluators defined for clustering.
  @Since("2.0.0")
  def computeCost(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val data: RDD[OldVector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }
    parentModel.computeCost(data)
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * For [[PrivateKMeansModelML]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   */
  @Since("1.6.0")
  override def write: MLWriter = new PrivateKMeansModelML.KMeansModelWriter(this)

  private var trainingSummary: Option[KMeansSummary] = None

  private[clustering] def setSummary(summary: KMeansSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
   * Return true if there exists summary of model.
   */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.0.0")
  def summary: KMeansSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("1.6.0")
object PrivateKMeansModelML extends MLReadable[PrivateKMeansModelML] {

  @Since("1.6.0")
  override def read: MLReader[PrivateKMeansModelML] = new KMeansModelReader

  @Since("1.6.0")
  override def load(path: String): PrivateKMeansModelML = super.load(path)

  /** Helper class for storing model data */
  private case class Data(clusterIdx: Int, clusterCenter: Vector)

  /**
   * We store all cluster centers in a single row and use this class to store model data by
   * Spark 1.6 and earlier. A model can be loaded from such older data for backward compatibility.
   */
  private case class OldData(clusterCenters: Array[OldVector])

  /** [[MLWriter]] instance for [[PrivateKMeansModelML]] */
  private[PrivateKMeansModelML] class KMeansModelWriter(instance: PrivateKMeansModelML) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: cluster centers
      val data: Array[Data] = instance.clusterCenters.zipWithIndex.map { case (center, idx) =>
        Data(idx, center)
      }
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
    }
  }

  private class KMeansModelReader extends MLReader[PrivateKMeansModelML] {

    /** Checked against metadata when loading model */
    private val className = classOf[PrivateKMeansModelML].getName

    override def load(path: String): PrivateKMeansModelML = {
      // Import implicits for Dataset Encoder
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val versionRegex = "([0-9]+)\\.(.+)".r
      val versionRegex(major, _) = metadata.sparkVersion

      val clusterCenters = if (major.toInt >= 2) {
        val data: Dataset[Data] = sparkSession.read.parquet(dataPath).as[Data]
        data.collect().sortBy(_.clusterIdx).map(_.clusterCenter).map(OldVectors.fromML)
      } else {
        // Loads PrivateKMeansModelML stored with the old format used by Spark 1.6 and earlier.
        sparkSession.read.parquet(dataPath).as[OldData].head().clusterCenters
      }
      val model = new PrivateKMeansModelML(metadata.uid, new MLlibPrivateKMeansModel(clusterCenters))
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * :: Experimental ::
 * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
 *
 * @see [[http://dx.doi.org/10.14778/2180912.2180915 Bahmani et al., Scalable k-means++.]]
 */
@Since("1.5.0")
@Experimental
class PrivateKMeansML @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[PrivateKMeansModelML] with KMeansParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> MLlibPrivateKMeans.K_MEANS_PARALLEL,
    initSteps -> 2,
    tol -> 1e-4)

  @Since("1.5.0")
  override def copy(extra: ParamMap): PrivateKMeansML = defaultCopy(extra)

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): PrivateKMeansModelML = {
    transformSchema(dataset.schema, logging = true)
    val rdd: RDD[OldVector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }

    val instr = Instrumentation.create(this, rdd)
    instr.logParams(featuresCol, predictionCol, k, initMode, initSteps, maxIter, seed, tol)

    val algo = new MLlibPrivateKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))
    val parentModel = algo.run(rdd, Option(instr))
    val model = copyValues(new PrivateKMeansModelML(uid, parentModel).setParent(this))
    val summary = new KMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k))
    model.setSummary(summary)
    instr.logSuccess(model)
    model
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("1.6.0")
object PrivateKMeansML extends DefaultParamsReadable[PrivateKMeansML] {

  @Since("1.6.0")
  override def load(path: String): PrivateKMeansML = super.load(path)
}

/**
 * :: Experimental ::
 * Summary of PrivateKMeansML.
 *
 * @param predictions  [[DataFrame]] produced by [[PrivateKMeansModelML.transform()]].
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 */
@Since("2.0.0")
@Experimental
class KMeansSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int) extends ClusteringSummary(predictions, predictionCol, featuresCol, k)
