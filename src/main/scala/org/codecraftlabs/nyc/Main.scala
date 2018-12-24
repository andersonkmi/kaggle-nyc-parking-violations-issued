package org.codecraftlabs.nyc

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.codecraftlabs.nyc.ParkingViolationsDataHandler.{ColumnNames, readContents, readPlatesContent, readStatesContent}
import org.codecraftlabs.nyc.data.{ParkingViolation, PlateType, State, ViolationCode}
import org.apache.spark.sql.functions._
import org.codecraftlabs.nyc.DataTransformationUtil.getCountByPlateType
import org.codecraftlabs.nyc.utils.ArgsUtils.parseArgs
import org.codecraftlabs.nyc.utils.Timer.{timed, timing}
import org.codecraftlabs.nyc.utils.NYCOpenDataUtils.getViolationCodeJsonArray

object Main {
  private val AppToken: String = "--app-token"
  private val CsvFolder: String = "--csv-folder"
  private val DataFolder: String = "--data-folder"

  private val columnsToFilter = Seq(
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
    "violationDescription",
    "vehicleYear"
  )

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    //Logger.getLogger("org.apache").setLevel(OFF)

    if (args.nonEmpty) {
      logger.info("Processing Kaggle NYC Parking violations data set")

      val argsMap = parseArgs(args)
      val csvFolder = argsMap(CsvFolder)
      val dataFolder = argsMap(DataFolder)
      val appToken = argsMap(AppToken)

      val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
      import sparkSession.implicits._

      logger.info("Loading the violation codes information from NYC Open data API")
      val violationCodesJsonArray = timed("Retrieving violation codes from NYC open data API", getViolationCodeJsonArray(appToken))
      val violationCodesDF = timed("Creating a data frame from the JSON array", sparkSession.createDataFrame(violationCodesJsonArray))
      val violationCodeModDF = timed("Converting code column from string to int", violationCodesDF.withColumn("violationCodeNumber", violationCodesDF.col("code").cast(IntegerType)).drop("code").drop("all_other_areas").drop("manhattan_96th_st_below").withColumnRenamed("violationCodeNumber", "code"))
      val violationCodeDS : Dataset[ViolationCode] = timed("Creating a dataset from the data frame", violationCodeModDF.as[ViolationCode])
      violationCodeDS.show(10)

      logger.info("Loading plates.csv data set")
      val plateTypeDS = timed("Reading plates.csv contents and converting its data frame to data set", readPlatesContent(s"$csvFolder/plates.csv", sparkSession).as[PlateType])
      plateTypeDS.show(10)

      logger.info("Loading state.csv data set")
      val stateDS = timed("Reading states.csv contents and converting the data frame to data set", readStatesContent(s"$csvFolder/states.csv", sparkSession).as[State])
      stateDS.show(10)

      logger.info("Loading all parking violations and transforming it")
      val violationsDataFrame = timed("Reading all parking violations", readContents(s"$dataFolder/*.csv", sparkSession))
      val resultingDF = violationsDataFrame.toDF(ColumnNames: _*)

      logger.info("Filtering only columns to be used")
      val filteredDF = timed("Filtering only the desired columns to be used later", resultingDF.select(resultingDF.columns.filter(colName => columnsToFilter.contains(colName)).map(colName => new Column(colName)): _*))
      val removedNullsDF = timed("Removing rows where the summons number is null", filteredDF.filter(filteredDF.col("summonsNumber").isNotNull))
      val modifiedDF = timed("Converting the timestamp field from string to timestamp", removedNullsDF.withColumn("issueDateTemp", unix_timestamp(removedNullsDF.col("issueDate"), "MM/dd/yyyy").cast(TimestampType))
        .drop("issueDate")
        .withColumnRenamed("issueDateTemp", "issueDate"))

      logger.info("Adding columns for year, month and date")
      val addedCols = timed("Adding specific columns for year, month and date", modifiedDF
        .withColumn("issueDayMonth", dayofmonth(modifiedDF.col("issueDate")))
        .withColumn("issueMonth", month(modifiedDF.col("issueDate")))
        .withColumn("issueYear", year(modifiedDF.col("issueDate"))))

      logger.info("Treating null value for violation description column")
      val colsForNullHandling = Seq("violationDescription")
      val naHandledDF = addedCols.na.fill("NA", colsForNullHandling)

      val violations: Dataset[ParkingViolation] = naHandledDF.as[ParkingViolation]
      violations.show(5000)

      // Split violations by year
      val violations2018 = timed("Filtering violations by year 2018", DataTransformationUtil.filterByYear(violations, 2018, sparkSession))
      timed("Counting rows", println(violations2018.count()))

      // Counting violations per plate type
      val byPlateType = timed("Counting violations by plate type", getCountByPlateType(violations, plateTypeDS, sparkSession))
      byPlateType.coalesce(1).write.mode("overwrite").json("violation_by_plate_type_all.json")

      println(timing)
    } else {
      println("Missing arguments.")
      logger.error("Missing arguments.")
    }
  }
}
