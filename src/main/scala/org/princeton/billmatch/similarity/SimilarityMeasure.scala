package org.princeton.billmatch
package similarity

import breeze.linalg.norm
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}

import org.apache.spark.ml.linalg.LinalgShim

/**
 * This abstract base class provides the interface for
 * similarity measures to be used in computing the actual
 * similarities between candidate pairs.
 *
 * It's framed in terms of similarity rather than distance
 * to provide a common interface that works for Cosine, Jaccard, hamming and Manhattan
 * similarity along with other similarities. 
 */

abstract class SimilarityMeasure extends Serializable {
  def compute(v1: Vector, v2: Vector): Double
}

final object CosineSimilarity extends SimilarityMeasure {

  /**
   * Compute cosine similarity between vectors
   *
   * LinalgShim reaches into Spark's linear algebra
   * code to use a BLAS dot product. Could probably be
   * replaced with a direct invocation of the appropriate
   * BLAS method.
   */
  def compute(v1: Vector, v2: Vector): Double = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    100.0*(math.abs(dotProduct) / norms)
  }
}

final object ManhattanSimilarity extends SimilarityMeasure {

  /**
   * Compute Manhattan similarity between vectors using
   * Breeze vector operations
   */
  def compute(v1: Vector, v2: Vector): Double = {
    val b1 = LinalgShim.asBreeze(v1)
    val b2 = LinalgShim.asBreeze(v2)
    100.0/(1+norm(b1 - b2, 1.0))
  }
}

final object HammingSimilarity extends SimilarityMeasure {
  
  /**
   * Compute Hamming similarity between vectors on a bit-level
   *
   * Since Spark ML doesn't support binary vectors, this converts
   * sparse vectors to Byte arrays correponsing to underlying dense representations
   */
  def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

  def compute(v1: Vector, v2: Vector): Double = {
    if (v1.toSparse.indices.size < 10) {
       val b1 : Array[Byte] = v1.toDense.values.map(_.toByte)
       val b2 : Array[Byte] = v2.toDense.values.map(_.toByte)

       val dist = (b1.zip(b2).map((x: (Byte, Byte)) => numberOfBitsSet((x._1 ^ x._2).toByte))).sum
       100.0/(1.0+dist)
    } else {
       val dist = (v1.toDense.values zip v2.toDense.values) count (x => x._1 != x._2)
       100.0/(1.0+dist)
    }
  } 
}


final object JaccardSimilarity extends SimilarityMeasure {

  /**
   * Compute Jaccard similarity between vectors
   *
   * Since Spark ML doesn't support binary vectors, this uses
   * sparse vectors and considers any active (i.e. non-zero)
   * index to represent a member of the set
   */
  def compute(v1: Vector, v2: Vector): Double = {
    val indices1 = v1.toSparse.indices.toSet
    val indices2 = v2.toSparse.indices.toSet
    val intersection = indices1.intersect(indices2).size.toDouble
    val union = indices1.size+indices2.size-intersection
    intersection/union*100.0
  }
}


final object DenseJaccardSimilarity extends SimilarityMeasure {

  def compute(v1: Vector, v2: Vector) : Double = {
     val s = v1.toDense.values.zip(v2.toDense.values) count (x => x._1 != x._2)
     val d = v1.size
     100.0*(d-s).toDouble/d
  }
}
