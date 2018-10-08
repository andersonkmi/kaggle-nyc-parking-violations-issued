import Dependencies._

organization := "org.codecraftlabs.spark"

name := "kaggle-nyc-parking-violations-issued"

val appVersion = "1.0.0.0"

val appName = "kaggle-nyc-parking-violations-issued"

version := appVersion

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.2",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.2",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.401",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  scalaTest % Test
)