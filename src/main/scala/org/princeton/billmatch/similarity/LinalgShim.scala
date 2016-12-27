package org.apache.spark.ml.linalg

import breeze.linalg.{ SparseVector => BSV, Vector => BV }

object LinalgShim {

  /**
   * Compute the dot product between two vectors
   *
   * Under the hood, Spark's BLAS module calls a BLAS routine
   * from netlib-java for the case of two dense vectors, or an
   * optimized Scala implementation in the case of sparse vectors.
   */
  def dot(x: Vector, y: Vector): Double = {
    BLAS.dot(x, y)
  }

  /**
   * Convert a Spark vector to a Breeze vector to access
   * vector operations that Spark doesn't provide.
   */
  def asBreeze(x: Vector): BV[Double] = {
    x.asBreeze
  }
}
