# How to trace a bill

First, check if a bill is in the input file using command line bash tools:

```bash
[alexeys@bd ~]$ cat /scratch/network/shiraito/text/data/jsonFiles/FL_MI_SC_metadata.json | grep FL_2005_SB436_
{"docversion": "Chaptered", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Chaptered", "year": 2005}
{"docversion": "Engrossed", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Engrossed", "year": 2005}
{"docversion": "Enrolled", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Enrolled", "year": 2005}
{"docversion": "Introduced", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Introduced", "year": 2005}
{"docversion": "Prefiled", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Prefiled", "year": 2005}
{"docversion": "Substituted", "state": 8, "docid": "SB436", "primary_key": "FL_2005_SB436_Substituted", "year": 2005}
```

Having ran the `MakeCartesian` step, inspect the output file in spark-shell:

Start the spark-shell including the BillAnalysis jar to be able to read custom object types like `CartesianPair`:  

```bash
spark-shell --master yarn-client --num-executors 20 --executor-cores 2 --jars target/scala-2.10/BillAnalysis-assembly-1.0.jar
```

As we know, `CartesianPair` is simply a pair of strings:

```scala
case class CartesianPair(pk1: String, pk2: String)
```
(in fact, we could use a Tuple2[String,String] instead).

thus, all we need to do is to loop over all of the pairs and see if the bill in question (FL/2005/SB436) 
is either first or second element of the pair:

```scala
val pairs = sc.objectFile[CartesianPair]("/user/alexeys/valid_pairs_FL_MI_SC")
val filtered_pairs = pairs.filter(x => ((x.pk1 contains "FL_2005_SB436_") || (x.pk2 contains "FL_2005_SB436_")))
filtered_pairs.count()
```
If the count is greater than 0, then there is definitely something. If you want to know what is there, print them to the screen:

```scala
for (d <- filtered_pairs.collect()) {
    println(d)
}
```

# Is it possible that a bill is in the input JSON but not in the output?

It can happen for 2 reasons:

1) The bill in question did not have an `Introduced` version (or whatever version you specify in the configuration file, 
i.e.: `docVersion   = "Enacted"`). 

2) If *all* of the bills in your dataset are from the same state (for instance, FL only) but you are considering 
pairs from different states. Note: in the current release this is a default, in the next release afollowing flag is introduced:

```bash
onlyInOut    = false, 
```

To switch between in-out and in-out & in-in state modes.



