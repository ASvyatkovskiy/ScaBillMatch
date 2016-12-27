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

  def createGF(spark: SparkSession, matches: RDD[Tuple2[(String,String),Double]], rescaled_df: DataFrame) : GraphFrame = {
     import spark.implicits._

     val edges_df = matches.map({case ((k1,k2), v1)=>(k1, k2, v1)}).toDF("src","dst","similarity")
     edges_df.printSchema
     edges_df.show()

     val vertices_df = rescaled_df.select(col("primary_key").alias("id"), col("content"))
     vertices_df.printSchema
     vertices_df.show()
     GraphFrame(vertices_df,edges_df)
  }

  def pageRank(gr: GraphFrame) : Unit = { 
    //gr.vertices.filter("age > 35")
    //gr.edges.filter("similarity > 20")
    //gr.inDegrees.filter("inDegree >= 2")
    val results = gr.pageRank.resetProbability(0.01).run()
    results.vertices.select("primary_key", "pagerank").show()
  }
}
