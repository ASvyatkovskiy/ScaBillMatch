name := "BillAnalysis"

version := "2.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  ("org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"),
  ("org.apache.spark"  % "spark-sql_2.11" % "2.2.0" % "provided"),
  ("org.apache.spark"  % "spark-mllib_2.11" % "2.2.0" % "provided")
)

//libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
libraryDependencies += "org.diana-hep" %% "histogrammar" % "1.0.4"
libraryDependencies += "org.diana-hep" %% "histogrammar-sparksql" % "1.0.4"
