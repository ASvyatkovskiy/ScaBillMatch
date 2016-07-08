import breeze.linalg.norm
import org.apache.spark.mllib.linalg.{ SparseVector, Vectors }

import org.apache.spark.mllib.linalg.LinalgShim

/**
 * This abstract base class provides the interface for
 * similarity measures to be used in computing the actual
 * similarities between candidate pairs.
 *
 * It's framed in terms of similarity rather than similarity
 * to provide a common interface that works for Euclidean
 * similarity along with other similarities. (Cosine similarity is
 * admittedly not a proper similarity measure, but is computed
 * similarly nonetheless.)
 */
private abstract class SimilarityMeasure extends Serializable {
  def compute(v1: SparseVector, v2: SparseVector): Double
}

private final object CosineSimilarity extends SimilarityMeasure {

  /**
   * Compute cosine similarity between vectors
   *
   * LinalgShim reaches into Spark's private linear algebra
   * code to use a BLAS dot product. Could probably be
   * replaced with a direct invocation of the appropriate
   * BLAS method.
   */
  def compute(v1: SparseVector, v2: SparseVector): Double = {
    val dotProduct = LinalgShim.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    100.0*(math.abs(dotProduct) / norms)
  }
}

private final object ManhattanSimilarity extends SimilarityMeasure {

  /**
   * Compute Manhattan similarity between vectors using
   * Breeze vector operations
   */
  def compute(v1: SparseVector, v2: SparseVector): Double = {
    val b1 = LinalgShim.toBreeze(v1)
    val b2 = LinalgShim.toBreeze(v2)
    100.0/(1+norm(b1 - b2, 1.0))
  }
}

private final object HammingSimilarity extends SimilarityMeasure {
  
  /**
   * Compute Hamming similarity between vectors on a bit-level
   *
   * Since MLlib doesn't support binary vectors, this converts
   * sparse vectors to Byte arrays correponsing to underlying dense representations
   */
  def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

  def compute(v1: SparseVector, v2: SparseVector): Double = {

    val b1 : Array[Byte] = v1.toDense.values.map(_.toByte)
    val b2 : Array[Byte] = v2.toDense.values.map(_.toByte)

    val dist = (b1.zip(b2).map((x: (Byte, Byte)) => numberOfBitsSet((x._1 ^ x._2).toByte))).sum
    100.0/(1.0+dist)
  } 
}


private final object JaccardSimilarity extends SimilarityMeasure {

  /**
   * Compute Jaccard similarity between vectors
   *
   * Since MLlib doesn't support binary vectors, this uses
   * sparse vectors and considers any active (i.e. non-zero)
   * index to represent a member of the set
   */
  def compute(v1: SparseVector, v2: SparseVector): Double = {
    val indices1 = v1.indices.toSet
    val indices2 = v2.indices.toSet
    val intersection = indices1.intersect(indices2).size.toDouble
    val union = indices1.size+indices2.size-intersection
    intersection/union*100.0
  }
}
