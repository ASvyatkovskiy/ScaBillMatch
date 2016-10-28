import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.sql.types.{StructType, ArrayType, DoubleType}
import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SparkSession

//import breeze.linalg.{DenseVector => BDV}
//import org.apache.spark.ml.linalg.{Vector, Vectors}


object UDAFTest {

  //get type of var utility 
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  class VectorSum (n: Int) extends UserDefinedAggregateFunction {
    def inputSchema = new StructType().add("v", new VectorUDT())
    def bufferSchema = new StructType().add("buff", ArrayType(DoubleType))
    def dataType = new VectorUDT()
    def deterministic = true 

    def initialize(buffer: MutableAggregationBuffer) = {
      buffer.update(0, Array.fill(n)(0.0))
    }

    def update(buffer: MutableAggregationBuffer, input: Row) = {
      if (!input.isNullAt(0)) {
        val buff = buffer.getAs[WrappedArray[Double]](0) 
        val v = input.getAs[Vector](0).toSparse
        for (i <- v.indices) {
          buff(i) += v(i)
        }
        buffer.update(0, buff)
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
      val buff1 = buffer1.getAs[WrappedArray[Double]](0) 
      val buff2 = buffer2.getAs[WrappedArray[Double]](0) 
      for ((x, i) <- buff2.zipWithIndex) {
        buff1(i) += x
      }
      buffer1.update(0, buff1)
    }

    def evaluate(buffer: Row) =  Vectors.dense(
      buffer.getAs[Seq[Double]](0).toArray)
  }


  def main (args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("BillAnalysis")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(Seq(
      (1, "[0,0,5]"), (1, "[4,0,1]"), (1, "[1,2,1]"),
      (2, "[7,5,0]"), (2, "[3,3,4]"), (3, "[0,8,1]"),
      (3, "[0,0,1]"), (3, "[7,7,7]")))

    val df = rdd.map{case (k, v) => (k, Vectors.parse(v))}.toDF("id", "vec")
    df.printSchema()
    df.show()
    for (d <- df.rdd.map(row => row.toSeq).take(5)) {
       val ret = d.asInstanceOf[WrappedArray[Any]]
       val first = ret(0).asInstanceOf[Integer]
       val second = ret(1).asInstanceOf[Vector]
       println(manOf(d))
       println(manOf(first))
       println(manOf(second))
    }

    val ca = new VectorSum(3) 
    val aggregated = df.groupBy($"id").agg(ca($"vec").alias("vec"))

     // df
     // .rdd
     // .map{ case Row(k: Int, v: Vector) => (k, BDV(v.toDense.values)) }
     // .foldByKey(BDV(Array.fill(3)(0.0)))(_ += _)
     // .mapValues(v => Vectors.dense(v.toArray))
     // .toDF("id", "vec")

    aggregated.show

  }

} 

