# List of states analyzed

CO, IL, NJ, FL, MI, SC, NY, CA, IN, TX

The processed dataset for 10 states can be found here on HDFS:

```bash
/user/alexeys/output_cosine_10
/user/alexeys/output_cosine_10_p2
/user/alexeys/output_cosine_10_p3
```

This is stored as object files with the following schema: `Tuple2[Tuple2[String,String],Double]` (or, ((key1,key2), similarity).
A minimum cut of similarity measure is applied (for cosine similarity it is 20.0).

To test the distribution of documents over k-means clusters, check the folloowing Parquet files on HDFS:

```bash
/user/alexeys/bills_combined_10
```

See this page for analysis of k-means and notes on tuning: https://github.com/ASvyatkovskiy/ScaBillMatch/blob/master/docs/tuning_kmeans.md

# Top similarity pairs for all-against-all

It is very easy to get top similarity pairs in spark-shell:

```scala
val data_cosine = sc.objectFile[Tuple2[Tuple2[String,String],Double]]("/user/alexeys/output_cosine_10*").cache()
val top_pairs_all = data_cosine.filter({case ((k1,k2),v) => (v > 70.0)}).cache()
val sorted_top_pairs_all = top_pairs_all.map(x => x.swap).sortByKey(false)
```

```scala
scala> for (d <- sorted_top_pairs_all.take(50)) {
     | println(d)
     | }
(99.93554537384274,(IL_2011_HB90_Introduced,MI_2009_SB1246_Introduced))         
(99.92544558786804,(IL_2011_HB90_Introduced,MI_2013_HB5741_Introduced))
(99.90848461011515,(IL_2007_HB676_Introduced,TX_2005_HB2613_Introduced))
(99.90512099549882,(IL_2005_HB499_Introduced,TX_2005_HB2613_Introduced))
(99.89645634432092,(IL_2003_HB4468_Introduced,TX_2005_HB2613_Introduced))
(99.88835626458777,(IL_2009_SB2544_Introduced,TX_2005_HB2613_Introduced))
(99.8323633646808,(IL_2007_SB50_Introduced,MI_2005_HB6657_Introduced))
(99.8267022780776,(IL_2007_SB50_Introduced,MI_2007_HB4336_Introduced))
(99.81272586794589,(IL_2007_SB50_Introduced,MI_2007_SB212_Introduced))
(99.80843339911353,(CO_2004_HB1304_Introduced,MI_2003_HB4145_Introduced))
(99.80447597831981,(IL_2007_SB50_Introduced,MI_2007_HB4343_Introduced))
(99.80433486834583,(IL_2001_HB3058_Introduced,NJ_2000_SB1201_Introduced))
(99.8016738545966,(IL_2001_HB3058_Introduced,NJ_2000_AB3560_Introduced))
(99.79359119612789,(IL_2007_HB375_Introduced,MI_2005_HB6657_Introduced))
(99.78866527292641,(IL_2005_HB4644_Introduced,MI_2005_HB6657_Introduced))
(99.78832629228899,(IL_2007_HB375_Introduced,MI_2007_HB4336_Introduced))
(99.78470850690852,(CA_2007_AB1809_Introduced,MI_2007_HB5960_Introduced))
(99.78470850690852,(CA_2007_AB1809_Introduced,MI_2007_HB5960_Introduced))
(99.78333385627441,(IL_2005_HB4644_Introduced,MI_2007_HB4336_Introduced))
(99.77993298175262,(CA_1995_SB1091_Introduced,MI_1995_SB1017_Introduced))
(99.77750918733152,(CA_2011_SB1449_Introduced,TX_2005_HB2613_Introduced))
(99.77672172235299,(IL_2007_HB375_Introduced,MI_2007_SB212_Introduced))
(99.77238626226567,(CA_2011_SB1449_Introduced,IL_2007_HB676_Introduced))
(99.77172961580624,(IL_2005_HB4644_Introduced,MI_2007_SB212_Introduced))
(99.76851459633002,(CA_2011_SB1449_Introduced,IL_2005_HB499_Introduced))
(99.76658379010208,(IL_1995_HB2337_Introduced,TX_1995_HB914_Introduced))
(99.76609973245327,(IL_2007_HB375_Introduced,MI_2007_HB4343_Introduced))
(99.76110842415399,(IL_2005_HB4644_Introduced,MI_2007_HB4343_Introduced))
(99.75678039784985,(CA_2011_SB1449_Introduced,IL_2009_SB2544_Introduced))
(99.75080072182577,(CA_2011_SB1449_Introduced,IL_2003_HB4468_Introduced))
(99.73444083861858,(CA_2007_SB1434_Introduced,TX_2005_HB2613_Introduced))
(99.72176563598255,(CA_2007_SB1434_Introduced,IL_2007_HB676_Introduced))
(99.71953303796411,(IL_2007_HB375_Introduced,IN_2008_HB1060_Introduced))
(99.71773210541704,(CA_2007_SB1434_Introduced,IL_2005_HB499_Introduced))
(99.71461575827118,(IL_2005_HB4644_Introduced,IN_2008_HB1060_Introduced))
(99.71314077499682,(FL_2009_HB1409_Introduced,IL_2011_HB90_Introduced))
(99.7108837944912,(IL_2013_SB31_Introduced,MI_2013_SB714_Introduced))
(99.70797413370724,(IL_2013_HB1029_Introduced,MI_2013_SB714_Introduced))
(99.70756044079275,(IL_2013_HB1239_Introduced,MI_2013_SB714_Introduced))
(99.70628801811647,(IL_2007_SB50_Introduced,IN_2008_HB1060_Introduced))
(99.70472738124059,(CA_2007_SB1434_Introduced,IL_2003_HB4468_Introduced))
(99.69330868188538,(CA_2007_AB1809_Introduced,MI_2007_HB5963_Introduced))
(99.69330868188538,(CA_2007_AB1809_Introduced,MI_2007_HB5963_Introduced))
(99.67551995992697,(CA_2007_SB1434_Introduced,IL_2009_SB2544_Introduced))
(99.67513001063682,(IN_2008_HB1060_Introduced,MI_2005_HB6657_Introduced))
(99.67513001063682,(IN_2008_HB1060_Introduced,MI_2005_HB6657_Introduced))
(99.67232919679738,(FL_2009_HB1409_Introduced,MI_2009_SB1246_Introduced))
(99.6696366246919,(IN_2008_HB1060_Introduced,MI_2007_HB4336_Introduced))
(99.6696366246919,(IN_2008_HB1060_Introduced,MI_2007_HB4336_Introduced))
(99.66873413391887,(IN_2008_HB1060_Introduced,NY_2007_AB7266_Introduced))
```

# Stand your ground bill tests

Following is a repeat of the stand your ground bill tests:

```scala
scala> val filtered_data_FL = data_cosine.filter({case ((k1,k2),v) => ((k1 contains "FL_2005_SB436_Introduced") || (k2 contains "FL_2005_SB436_Introduced"))}).cache()
filtered_data_FL: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[10] at filter at <console>:26

scala> val filtered_data_FL = data_cosine.filter({case ((k1,k2),v) => (v > 50.0)}).filter({case ((k1,k2),v) => ((k1 contains "FL_2005_SB436_Introduced") || (k2 contains "FL_2005_SB436_Introduced"))}).cache()
filtered_data_FL: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[12] at filter at <console>:26

scala> val sorted_data_FL = filtered_data_FL.map(x => x.swap).sortByKey(false)
sorted_data_FL: org.apache.spark.rdd.RDD[(Double, (String, String))] = ShuffledRDD[16] at sortByKey at <console>:28

scala> for (d <- sorted_data_FL.take(50)) {
     | println(d)
     | }
(91.191236730442,(FL_2005_SB436_Introduced,MI_2005_HB5143_Introduced))          
(90.36916590249547,(FL_2005_SB436_Introduced,MI_2005_SB1046_Introduced))
(90.02658104358225,(FL_2005_SB436_Introduced,MI_2005_HB5153_Introduced))
(77.91866908768156,(FL_2005_SB436_Introduced,SC_2005_HB4301_Introduced))
(77.90284432627624,(FL_2005_SB436_Introduced,SC_2005_SB1131_Introduced))
(75.1876890235078,(FL_2005_SB436_Introduced,NY_2007_AB8182_Introduced))
(74.20987396112983,(FL_2005_SB436_Introduced,NY_2007_SB7173_Introduced))
(70.48714718179238,(FL_2005_SB436_Introduced,NJ_2006_AB134_Introduced))
(70.46379476814334,(FL_2005_SB436_Introduced,NJ_2004_AB4125_Introduced))
(70.45688625287595,(FL_2005_SB436_Introduced,NJ_2008_SB1843_Introduced))
(70.42146244621087,(FL_2005_SB436_Introduced,NJ_2008_AB159_Introduced))
(70.41192396081183,(FL_2005_SB436_Introduced,NJ_2010_SB555_Introduced))
(70.26820922162828,(FL_2005_SB436_Introduced,NJ_2012_AB886_Introduced))
(70.24096571535175,(FL_2005_SB436_Introduced,NJ_2010_AB198_Introduced))
(69.27676577415795,(FL_2005_SB436_Introduced,SC_2009_HB3025_Introduced))
(66.52780171616635,(FL_2005_SB436_Introduced,NJ_2008_SB2641_Introduced))
(66.52205253881436,(FL_2005_SB436_Introduced,NJ_2010_SB1628_Introduced))
(66.50852916378521,(FL_2005_SB436_Introduced,NJ_2014_SB792_Introduced))
(66.50743870578077,(FL_2005_SB436_Introduced,NJ_2012_SB707_Introduced))
(66.48466147777326,(FL_2005_SB436_Introduced,NJ_2008_AB3794_Introduced))
(66.40918756579721,(FL_2005_SB436_Introduced,NJ_2010_AB421_Introduced))
(66.39211435085558,(FL_2005_SB436_Introduced,NJ_2012_AB1906_Introduced))
(66.3120960262355,(FL_2005_SB436_Introduced,NJ_2014_AB1861_Introduced))
(53.15903691016709,(FL_2005_SB436_Introduced,TX_2013_SB1349_Introduced))
(51.069039379047034,(FL_2005_SB436_Introduced,IN_2014_SB46_Introduced))
```

Which is in a close agreement with the 3 state test.
