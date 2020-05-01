name := "spark_nlp_analysis"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.4.5"
)