package org.princeton.billmatch
package linalg

//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._

import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,CoordinateMatrix}

import org.apache.spark.mllib.linalg.{
  Vector => OldVector,
  Vectors => OldVectors,
  SparseVector => OldSparseVector,
  DenseVector => OldDenseVector,
  VectorUDT => OldVectorUDT}

import org.apache.spark.ml.linalg.{
   Vector => NewVector,
   Vectors => NewVectors,
   DenseVector => NewDenseVector,
   SparseVector => NewSparseVector
}

object LinalgUtils { 

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

  //Calculate submatrix containing nColsToKeep of inital matrix A. This is used for truncating the V.T matrix in SVD 
  def truncatedMatrix(a: Matrix, nColsToKeep: Int): Matrix = {
      Matrices.dense(a.numRows,nColsToKeep,a.toArray.slice(0,a.numRows*nColsToKeep))
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

}
