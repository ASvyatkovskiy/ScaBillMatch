name := "BillAnalysis"

version := "2.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  ("org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.mail").
    exclude("org.eclipse.jetty.orbit", "javax.activation").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.google.guava","guava").
    exclude("org.apache.hadoop","hadoop-yarn-api").
    exclude("com.esotericsoftware.minlog", "minlog"),
  ("org.apache.spark"  % "spark-sql_2.11" % "2.0.0" % "provided"),
  ("org.apache.spark"  % "spark-mllib_2.11" % "2.0.0" % "provided"),
  ("org.apache.lucene" % "lucene-analyzers-common"      % "5.1.0")
)

//libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
