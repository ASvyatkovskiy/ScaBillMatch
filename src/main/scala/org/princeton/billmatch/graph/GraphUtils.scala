package org.princeton.billmatch
package graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import org.apache.spark.rdd.RDD

import org.graphframes._

object GraphUtils {

  def createGF(spark: SparkSession, matches: RDD[Tuple2[(String,String),Double]], df: DataFrame) : GraphFrame = {
     import spark.implicits._

     val edges_df = matches.map({case ((k1,k2), v1)=>(k1, k2, v1)}).toDF("src","dst","similarity")
     val vertices_df = df.select(col("primary_key").alias("id"), col("content"))
     GraphFrame(vertices_df,edges_df)
  }

  def runPageRank(gf: GraphFrame, prob: Double, maxIterations: Integer, getEdgeWeights: Boolean = false) : DataFrame = { 
    val results = gf.pageRank.resetProbability(prob).maxIter(maxIterations).run()
    if (getEdgeWeights) {
       results.edges.select("src", "dst", "weight")
    } else {
       results.vertices.select("id", "pagerank")
    }
  }

  def runShortestPaths(gf: GraphFrame, from_vertex: String, to_vertex: String) : DataFrame = {
    val results = gf.shortestPaths.landmarks(Seq(from_vertex,to_vertex)).run()
    results.select(col("id_from"), explode(col("distances")).as("id_to" :: "distances" :: Nil))
  }

  def runTriangleCount(gf: GraphFrame) : DataFrame = {
    val results = gf.triangleCount.run() 
    results.select("id", "count")
  }
}
