import Dependencies._

organization := "org.codecraftlabs.spark"

name := "kaggle-nyc-parking-violations"

val appVersion = "1.0.0"

val appName = "kaggle-nyc-parking-violations"

version := appVersion

scalaVersion := "2.12.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.json4s" %% "json4s-jackson" % "3.6.2",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
  "org.codecraftlabs.spark" %% "spark-utils" % "1.0.3",
  scalaTest % Test
)