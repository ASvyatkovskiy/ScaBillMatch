Following is a raw test of GraphFrames in spark-shell during the development if GraphUtils sub-package.


```scala
scala> import org.graphframes._
import org.graphframes._                                                       

scala> val matches = sc.objectFile[Tuple2[(String,String),Double]]("/user/alexeys/10_STATE_RESULTS/output_cosine_10_noU_p1/")
matches: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[107] at objectFile at <console>:27

scala> val edges_df = matches.map({case ((k1,k2), v1)=>(k1, k2, v1)}).toDF("src","dst","similarity")
edges_df: org.apache.spark.sql.DataFrame = [src: string, dst: string ... 1 more field]

scala> edges_df.show()
+--------------------+--------------------+------------------+                  
|                 src|                 dst|        similarity|
+--------------------+--------------------+------------------+
|FL_1993_HB1177_In...|MI_1993_SB612_Int...| 25.28675508643724|
|IL_2009_HB5363_In...|MI_1993_SB612_Int...| 32.49607528361805|
|FL_2003_HB685_Int...|MI_1993_SB612_Int...|28.554461899840465|
|FL_1999_SB2178_In...|MI_1993_SB612_Int...| 62.78593064930907|
|IL_2011_HB5707_In...|MI_1993_SB612_Int...|  53.8599784730097|
|FL_2011_SB2028_In...|MI_1993_SB612_Int...| 31.50344652916427|
|FL_2003_SB1392_In...|MI_1993_SB612_Int...| 34.79762312536053|
|IL_1993_SB1726_In...|MI_1993_SB612_Int...| 42.30804284162574|
|FL_2004_HB55_Intr...|MI_1993_SB612_Int...|  45.5526018708176|
|IL_2009_SB3317_In...|MI_1993_SB612_Int...|32.359251063512815|
|FL_2000_HB891_Int...|MI_1993_SB612_Int...|28.987161354342906|
|IL_2009_HB618_Int...|MI_1993_SB612_Int...| 53.81603170766404|
|FL_2004_SB592_Int...|MI_1993_SB612_Int...|33.524725297293266|
|CO_2011_SB233_Int...|MI_1993_SB612_Int...| 64.68644017785088|
|FL_2003_HB1841_In...|MI_1993_SB612_Int...|37.900712002291655|
|FL_2004_SB628_Int...|MI_1993_SB612_Int...|36.225302816656246|
|FL_2002_HB663_Int...|MI_1993_SB612_Int...| 34.13953122382638|
|FL_2003_SB68_Intr...|MI_1993_SB612_Int...| 30.68175143444832|
|CO_1998_SB65_Intr...|MI_1993_SB612_Int...|61.737215711328474|
|FL_1999_HB1457_In...|MI_1993_SB612_Int...|31.243825702679846|
+--------------------+--------------------+------------------+
only showing top 20 rows


scala> val raw_data = spark.read.json("file:///scratch/network/alexeys/bills/lexs/bills_combined_10_noU.json")
raw_data: org.apache.spark.sql.DataFrame = [content: string, docid: string ... 4 more fields]

scala> val vertices_df = raw_data.select(col("primary_key").alias("id"), col("content"))
vertices_df: org.apache.spark.sql.DataFrame = [id: string, content: string]

scala> val gf = GraphFrame(vertices_df,edges_df)
gf: org.graphframes.GraphFrame = GraphFrame(v:[id: string, content: string], e:[src: string, dst: string ... 1 more field])

scala> val results = gf.pageRank.resetProbability(0.15).maxIter(5).run()
results: org.graphframes.GraphFrame = GraphFrame(v:[id: string, content: string ... 1 more field], e:[src: string, dst: string ... 2 more fields])

scala> results.vertices.select("id", "pagerank").show()
+--------------------+-------------------+                                      
|                  id|           pagerank|
+--------------------+-------------------+
|NY_1999_SB5475_In...|0.16564684006941416|
|TX_1999_HB2617_In...|               0.15|
|TX_1995_SCR105_Su...|               0.15|
|NJ_2008_ACR232_In...| 0.1685060576376553|
|TX_1999_HB3414_In...| 0.7332531544748746|
|NJ_2012_AB3092_In...|0.16540735016056904|
|TX_2007_HB1633_En...|               0.15|
|MI_2009_SB618_Int...|               0.15|
|IL_2003_SR768_Ado...|               0.15|
|TX_2003_SR1063_Ad...|               0.15|
|NJ_2014_AB2921_In...|0.16440156361077474|
|FL_1999_HB1633_In...|               0.15|
|TX_2001_SR568_Ado...|               0.15|
|TX_1997_HB1700_En...|               0.15|
|CA_2011_AB902_Int...|               0.15|
|TX_2007_HB4117_In...| 0.1783627052460395|
|CO_1994_SB66_Enac...|               0.15|
|FL_2009_SB542_Pre...|               0.15|
|NY_1999_AB1734_In...|0.26773678835175385|
|NY_2005_AB5285_In...|0.20385422281927823|
+--------------------+-------------------+
only showing top 20 rows


scala> results.edges.select("src", "dst", "weight").show()
+--------------------+--------------------+--------------------+                0]0]
|                 src|                 dst|              weight|
+--------------------+--------------------+--------------------+
|CO_2001_SJR2B_Int...|NY_1997_AB2053_In...| 0.01020408163265306|
|CA_2003_AB1730_In...|FL_2007_HB201_Int...| 0.00267379679144385|
|CA_2003_AB1730_In...|TX_2005_SB1668_In...| 0.00267379679144385|
|IL_2001_HB3044_In...|SC_2011_HB4994_In...|9.560229445506692E-4|
|IL_2001_HB3044_In...|TX_2005_SB33B_Int...|9.560229445506692E-4|
|IL_2001_HB3044_In...|MI_2005_HB4075_In...|9.560229445506692E-4|
|IL_2001_HB3044_In...|IN_2005_HB1154_In...|9.560229445506692E-4|
|IL_2001_HB3044_In...|TX_1999_SB1461_In...|9.560229445506692E-4|
|IL_2001_HB3044_In...|SC_1997_HB4848_In...|9.560229445506692E-4|
|IL_2001_HB3044_In...|MI_2003_SB951_Int...|9.560229445506692E-4|
|FL_2006_SB1878_In...|NY_2007_SB7488_In...|0.015151515151515152|
|MI_2009_HB4053_In...|NJ_1996_SJR12_Int...|0.001876172607879925|
|MI_2009_HB4053_In...|NY_2003_AB6465_In...|0.001876172607879925|
|MI_2009_HB4053_In...|NY_2005_AB10277_I...|0.001876172607879925|
|MI_2009_HB4053_In...|NY_1997_SB5562_In...|0.001876172607879925|
|NY_2007_SB2276_In...|TX_2011_HB3581_In...|0.003194888178913738|
|NY_2007_SB2276_In...|TX_2005_SB1416_In...|0.003194888178913738|
|IN_1998_HB1402_In...|NY_1993_SB5559_In...|0.003846153846153...|
|IN_1998_HB1402_In...|TX_2003_SB1493_In...|0.003846153846153...|
|IN_2006_HB1159_In...|NY_2013_AB9407_In...|5.321979776476849E-4|
+--------------------+--------------------+--------------------+
only showing top 20 rows

scala> val results_paths = gf.shortestPaths.landmarks(Seq("NY_1999_SB5475_Introduced","FL_1999_HB1633_Introduced")).run()
results_paths: org.apache.spark.sql.DataFrame = [id: string, content: string ... 1 more field]

scala> results_paths.select("id", "distances").show(false)
+--------------------------------+-----------------------------------+          
|id                              |distances                          |
+--------------------------------+-----------------------------------+
|NY_1999_SB5475_Introduced       |Map(NY_1999_SB5475_Introduced -> 0)|
|TX_1999_HB2617_Introduced       |Map()                              |
|TX_1995_SCR105_Substituted      |Map()                              |
|NJ_2008_ACR232_Introduced       |Map()                              |
|TX_1999_HB3414_Introduced       |Map()                              |
|NJ_2012_AB3092_Introduced       |Map()                              |
|TX_2007_HB1633_Engrossed        |Map()                              |
|MI_2009_SB618_Introduced        |Map()                              |
|IL_2003_SR768_Adopted           |Map()                              |
|TX_2003_SR1063_Adopted          |Map()                              |
|NJ_2014_AB2921_Introduced       |Map()                              |
|FL_1999_HB1633_Introduced       |Map(FL_1999_HB1633_Introduced -> 0)|
|TX_2001_SR568_Adopted           |Map()                              |
|TX_1997_HB1700_Engrossed        |Map()                              |
|CA_2011_AB902_Introduced        |Map()                              |
|TX_2007_HB4117_Introduced       |Map()                              |
|CO_1994_SB66_Enacted--MAY22,1994|Map()                              |
|FL_2009_SB542_Prefiled          |Map()                              |
|NY_1999_AB1734_Introduced       |Map()                              |
|NY_2005_AB5285_Introduced       |Map()                              |
+--------------------------------+-----------------------------------+
only showing top 20 rows


scala> results_paths.count()
res13: Long = 1299783                                                           

scala> val results_triangles = gf.triangleCount.run()
results_triangles: org.apache.spark.sql.DataFrame = [count: bigint, id: string ... 1 more field]

scala> results_triangles.show()
```
