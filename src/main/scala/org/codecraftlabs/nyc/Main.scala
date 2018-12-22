package org.codecraftlabs.nyc

import org.apache.log4j.Level.OFF
import org.apache.log4j.Logger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.codecraftlabs.nyc.ParkingViolationsDataHandler.{ColumnNames, readContents, readPlatesContent, readStatesContent}
import org.codecraftlabs.nyc.data.{ParkingViolation, PlateType, State}
import org.apache.spark.sql.functions._
import org.codecraftlabs.nyc.DataTransformationUtil.getCountByPlateType
import org.codecraftlabs.nyc.utils.Timer.{timed, timing}
import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse

object Main {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle NYC Parking violations data set")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val plateTypeDS = timed("Reading plates.csv contents and converting its data frame to data set", readPlatesContent("plates.csv", sparkSession).as[PlateType])
    plateTypeDS.show(100)
    plateTypeDS.printSchema()

    val stateDS = timed("Reading states.csv contents and converting the data frame to data set", readStatesContent("states.csv", sparkSession).as[State])
    stateDS.show(100)
    stateDS.printSchema()

    val df1 = timed("Reading parking-violations-issued-fiscal-year-2018.csv contents", readContents("parking-violations-issued-fiscal-year-2018.csv", sparkSession))
    val renamedDF = df1.toDF(ColumnNames: _*)

    val df2 = timed("Reading parking-violations-issued-fiscal-year-2016.csv contents", readContents("parking-violations-issued-fiscal-year-2016.csv", sparkSession))
    val renamedDF2 = df2.toDF(ColumnNames: _*)

    val df3 = timed("Reading parking-violations-issued-fiscal-year-2014.csv contents", readContents("parking-violations-issued-fiscal-year-2014.csv", sparkSession))
    val renamedDF3 = df3.toDF(ColumnNames: _*)

    val resultingDF = timed("Executing union on all three data frames loaded", renamedDF.union(renamedDF2).union(renamedDF3))

    val colsToKeep = Seq(
      "summonsNumber",
      "plateId",
      "registrationState",
      "plateType",
      "issueDate",
      "violationCode",
      "vehicleBodyType",
      "vehicleMake",
      "issuingAgency",
      "vehicleColor",
      "violationTime",
      "vehicleYear"
    )

    val filteredDF = timed("Filtering only the desired columns to be used later", resultingDF.select(df1.columns.filter(colName => colsToKeep.contains(colName)).map(colName => new Column(colName)): _*))
    val removedNullsDF = timed("Removing rows where the summons number is null", filteredDF.filter(filteredDF.col("summonsNumber").isNotNull))
    val modifiedDF = timed("Converting the timestamp field from string to timestamp", removedNullsDF.withColumn("issueDateTemp", unix_timestamp(removedNullsDF.col("issueDate"), "yyyy-MM-dd").cast(TimestampType))
      .drop("issueDate")
      .withColumnRenamed("issueDateTemp", "issueDate"))

    val addedCols = timed("Adding specific columns for year, month and date", modifiedDF
      .withColumn("issueDayMonth", dayofmonth(modifiedDF.col("issueDate")))
      .withColumn("issueMonth", month(modifiedDF.col("issueDate")))
      .withColumn("issueYear", year(modifiedDF.col("issueDate"))))
    val violations: Dataset[ParkingViolation] = addedCols.as[ParkingViolation]

    // Split violations by year
    val violations2018 = timed("Filtering violations by year 2018", violations.filter("issueYear == 2018"))
    timed("Counting rows", println(violations2018.count()))

    val byPlateType = timed("Counting violations by plate type", getCountByPlateType(violations, sparkSession))
    byPlateType.show(100)

    println(timing)
  }
}
