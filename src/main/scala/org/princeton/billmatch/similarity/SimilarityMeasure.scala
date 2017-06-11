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
  def compute(v1: Vector, v2: Vector): Float
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
  def compute(v1: Vector, v2: Vector): Float = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    (100.0*(math.abs(dotProduct) / norms)).toFloat
  }
}

final object ManhattanSimilarity extends SimilarityMeasure {

  /**
   * Compute Manhattan similarity between vectors using
   * Breeze vector operations
   */
  def compute(v1: Vector, v2: Vector): Float = {
    val b1 = LinalgShim.asBreeze(v1)
    val b2 = LinalgShim.asBreeze(v2)
    (100.0/(1+norm(b1 - b2, 1.0))).toFloat
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

  def compute(v1: Vector, v2: Vector): Float = {
    if (v1.toSparse.indices.size < 10) {
       val b1 : Array[Byte] = v1.toDense.values.map(_.toByte)
       val b2 : Array[Byte] = v2.toDense.values.map(_.toByte)

       val dist = (b1.zip(b2).map((x: (Byte, Byte)) => numberOfBitsSet((x._1 ^ x._2).toByte))).sum
       (100.0/(1.0+dist)).toFloat
    } else {
       val dist = (v1.toDense.values zip v2.toDense.values) count (x => x._1 != x._2)
       (100.0/(1.0+dist)).toFloat
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
  def compute(v1: Vector, v2: Vector): Float = {
    val indices1 = v1.toSparse.indices.toSet
    val indices2 = v2.toSparse.indices.toSet
    val intersection = indices1.intersect(indices2).size.toFloat
    val union = indices1.size+indices2.size-intersection
    //1 - intersectionSize / unionSize
    (intersection/union*100.0).toFloat
  }
}

final object weightedJaccardSimilarity extends SimilarityMeasure {

  def compute(v1: Vector, v2: Vector): Float = {
    val indices1 = v1.toSparse.indices.toSet
    val indices2 = v2.toSparse.indices.toSet
    val intersection = indices1.intersect(indices2).size.toFloat

    val xsize = indices1.size
    val ysize = indices2.size

    val relative = Math.abs(xsize-ysize).asInstanceOf[Float]/math.sqrt(xsize*ysize)
    val similarity: Float = relative match {
       case relative if relative > 5.0 => {
         val m = Math.min(xsize,ysize).toFloat
         val alpha: Float = m/math.max(xsize,ysize).toFloat
         val r: Float = intersection/m
         val weight: Float = ((1.0-r)*(1.0+alpha)/(1.0+r)/(1.0+alpha-2.0*alpha*r)).toFloat

         val b1 = LinalgShim.asBreeze(v1)
         val b2 = LinalgShim.asBreeze(v2)

         (100.0*(Vectors.norm(v1,1) + Vectors.norm(v2,1) - weight*norm(b1-b2,1.0))/(Vectors.norm(v1,1) + Vectors.norm(v2,1) + weight*norm(b1-b2,1.0))).toFloat
       }
       case _ => {
         val union = indices1.size+indices2.size-intersection
         (intersection/union*100.0).toFloat
       }
    }
    similarity
  }
}

final object LeftJaccardSimilarity extends SimilarityMeasure {

  def compute(v1: Vector, v2: Vector): Float = {
    val indices1 = v1.toSparse.indices.toSet
    val indices2 = v2.toSparse.indices.toSet
    val intersection = indices1.intersect(indices2).size.toFloat
    val union = indices1.size
    (intersection/union*100.0).toFloat
  }
}

final object RightJaccardSimilarity extends SimilarityMeasure {

  def compute(v1: Vector, v2: Vector): Float = {
    val indices1 = v1.toSparse.indices.toSet
    val indices2 = v2.toSparse.indices.toSet
    val intersection = indices1.intersect(indices2).size.toFloat
    val union = indices2.size
    (intersection/union*100.0).toFloat
  }
}


final object DenseJaccardSimilarity extends SimilarityMeasure {

  def compute(v1: Vector, v2: Vector) : Float = {
     val s = v1.toDense.values.zip(v2.toDense.values) count (x => x._1 != x._2)
     val d = v1.size
     (100.0*(d-s).toFloat/d).toFloat
  }
}
