package org.codecraftlabs.nyc

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.codecraftlabs.nyc.data.ParkingViolationsDataHandler.{ColumnNames, readContents, readPlatesContent, readStatesContent, _}
import org.codecraftlabs.nyc.data.{ParkingViolation, PlateType, State, ViolationCode}
import org.codecraftlabs.nyc.utils.DataTransformationUtil._
import org.codecraftlabs.nyc.utils.NYCOpenDataUtils.getViolationCodeJsonArray
import org.codecraftlabs.spark.utils.ArgsUtils._
import org.codecraftlabs.spark.utils.DataFrameUtil.saveDataFrameToJson
import org.codecraftlabs.spark.utils.DataSetUtil.saveDataSetToJson
import org.codecraftlabs.spark.utils.Timer._

object Main {
  private val AppToken: String = "--app-token"
  private val CsvFolder: String = "--csv-folder"
  private val DataFolder: String = "--data-folder"
  private val DestinationFolder: String = "--destination-folder"
  private val Partitions: String = "--partition-number"


  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)

    if (args.nonEmpty) {
      logger.info("Processing Kaggle NYC Parking violations data set")

      val argsMap = parseArgs(args)
      val csvFolder = argsMap(CsvFolder)
      val dataFolder = argsMap(DataFolder)
      val appToken = argsMap(AppToken)
      val destinationFolder = argsMap.getOrElse(DestinationFolder, ".") + "/"
      val partitions = argsMap.getOrElse(Partitions, "1").toInt

      val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
      import sparkSession.implicits._

      logger.info("Loading the violation codes information from NYC Open data API")
      val violationCodesJsonArray = timed("Retrieving violation codes from NYC open data API", getViolationCodeJsonArray(appToken))
      val violationCodesDF = timed("Creating a data frame from the JSON array", sparkSession.createDataFrame(violationCodesJsonArray))
      val violationCodeModDF = timed("Converting code column from string to int", violationCodesDF.withColumn("violationCodeNumber", violationCodesDF.col("code").cast(IntegerType)).drop("code").drop("all_other_areas").drop("manhattan_96th_st_below").withColumnRenamed("violationCodeNumber", "code"))
      val violationCodeDS : Dataset[ViolationCode] = timed("Creating a data set from the data frame", violationCodeModDF.as[ViolationCode])
      violationCodeDS.persist()

      logger.info("Loading plates.csv data set")
      val plateTypeDS = timed("Reading plates.csv contents and converting its data frame to data set", readPlatesContent(s"$csvFolder/plates.csv", sparkSession).as[PlateType])
      plateTypeDS.persist()

      logger.info("Loading state.csv data set")
      val stateDS = timed("Reading states.csv contents and converting the data frame to data set", readStatesContent(s"$csvFolder/states.csv", sparkSession).as[State])
      stateDS.persist()

      logger.info("Loading all parking violations and transforming it")
      val violationsDataFrame = timed("Reading all parking violations", readContents(s"$dataFolder/*.csv", sparkSession))
      val resultingDF = violationsDataFrame.toDF(ColumnNames: _*)

      logger.info("Filtering only columns to be used")
      val filteredDF = timed("Filtering only the desired columns to be used later", resultingDF.select(resultingDF.columns.filter(colName => ColumnsToFilter.contains(colName)).map(colName => new Column(colName)): _*))
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
      violations.persist()

      // Split violations by year
      val violations2019 = timed("Filtering violations by year 2019", filterByYear(violations, 2019, sparkSession))
      violations2019.persist()

      val violations2018 = timed("Filtering violations by year 2018", filterByYear(violations, 2018, sparkSession))
      violations2018.persist()

      val violations2017 = timed("Filtering violations by year 2017", filterByYear(violations, 2017, sparkSession))
      violations2017.persist()

      val violations2016 = timed("Filtering violations by year 2016", filterByYear(violations, 2016, sparkSession))
      violations2016.persist()

      val violations2015 = timed("Filtering violations by year 2015", filterByYear(violations, 2015, sparkSession))
      violations2015.persist()

      val violations2014 = timed("Filtering violations by year 2014", filterByYear(violations, 2014, sparkSession))
      violations2014.persist()

      // Counting violations per plate type
      val byPlateType = timed("Counting violations by plate type", countViolationsByPlateType(violations, plateTypeDS, sparkSession))
      val byPlateTypeSorted = byPlateType.sort(desc("count"))
      saveDataFrameToJson(byPlateTypeSorted.toDF(), s"${destinationFolder}violation_by_plate_type_all.json", partitions)

      // Count violations by plate registration
      val violationCountByState = timed("Counting violations by registration state", countViolationsByState(violations, stateDS, sparkSession))
      val sortedViolationCountByState = violationCountByState.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByState.toDF(), s"${destinationFolder}violation_count_by_registration_state.json", partitions)

      // Count violations by year
      val violationsByYear = timed("Counting violations by year", countViolationsByYear(violations, sparkSession))
      val sortedViolationCountByYear = violationsByYear.sort(desc("issueYear"))
      val filteredYears = filterViolationsFromYears(2014, 2019, sortedViolationCountByYear)
      saveDataFrameToJson(filteredYears.toDF(), s"${destinationFolder}violation_count_by_year.json", partitions)

      // Count violations by violation code
      val violationsByCode = timed("Counting violations by violation code", countViolationsByViolationCode(violations, sparkSession))
      val sortedViolationsByCode = violationsByCode.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByCode, s"${destinationFolder}violation_count_by_violation_code.json", partitions)

      // Count violations by violation definition
      val violationsByDefinition = timed("Counting violations by violation definition", countViolationsByViolationDefinition(violations, violationCodeDS, sparkSession))
      val sortedViolationsByDefinition = violationsByDefinition.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinition, s"${destinationFolder}violation_count_by_violation_definition.json", partitions)

      // Count violations per plate type for FY2019
      val violationsByPlateTypeFY2019 = timed("Counting violations by plate type - FY2019", countViolationsByPlateType(violations2019, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2019Sorted = violationsByPlateTypeFY2019.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2019Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2019.json", partitions)

      // Count violations by plate registration for FY2019
      val violationCountByStateFY2019 = timed("Counting violations by registration state - FY2019", countViolationsByState(violations2019, stateDS, sparkSession))
      val sortedViolationCountByStateFY2019 = violationCountByStateFY2019.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2019.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2019.json", partitions)

      // Count violations by code definition - fy2019
      val violationsByDefinitionFY2019 = timed("Counting violations by violation definition", countViolationsByViolationDefinition(violations2019, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2019 = violationsByDefinitionFY2019.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2019, s"${destinationFolder}violation_count_by_violation_definition_fy2019.json", partitions)

      // Count violations per plate type for FY2018
      val violationsByPlateTypeFY2018 = timed("Counting violations by plate type - FY2018", countViolationsByPlateType(violations2018, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2018Sorted = violationsByPlateTypeFY2018.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2018Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2018.json", partitions)

      // Count violations by plate registration for FY2018
      val violationCountByStateFY2018 = timed("Counting violations by registration state - FY2018", countViolationsByState(violations2018, stateDS, sparkSession))
      val sortedViolationCountByStateFY2018 = violationCountByStateFY2018.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2018.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2018.json", partitions)

      // Count violations by code definition - fy2018
      val violationsByDefinitionFY2018 = timed("Counting violations by violation definition - FY2018", countViolationsByViolationDefinition(violations2018, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2018 = violationsByDefinitionFY2018.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2018, s"${destinationFolder}violation_count_by_violation_definition_fy2018.json", partitions)

      // Count violations per plate type for FY2017
      val violationsByPlateTypeFY2017 = timed("Counting violations by plate type - FY2017", countViolationsByPlateType(violations2017, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2017Sorted = violationsByPlateTypeFY2017.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2017Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2017.json", partitions)

      // Count violations by plate registration for FY2017
      val violationCountByStateFY2017 = timed("Counting violations by registration state - FY2017", countViolationsByState(violations2017, stateDS, sparkSession))
      val sortedViolationCountByStateFY2017 = violationCountByStateFY2017.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2017.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2017.json", partitions)

      // Count violations by code definition - fy2017
      val violationsByDefinitionFY2017 = timed("Counting violations by violation definition - FY2017", countViolationsByViolationDefinition(violations2017, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2017 = violationsByDefinitionFY2017.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2017, s"${destinationFolder}violation_count_by_violation_definition_fy2017.json", partitions)

      // Count violations per plate type for FY2016
      val violationsByPlateTypeFY2016 = timed("Counting violations by plate type - FY2016", countViolationsByPlateType(violations2016, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2016Sorted = violationsByPlateTypeFY2016.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2016Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2016.json", partitions)

      // Count violations by plate registration for FY2016
      val violationCountByStateFY2016 = timed("Counting violations by registration state - FY2016", countViolationsByState(violations2016, stateDS, sparkSession))
      val sortedViolationCountByStateFY2016 = violationCountByStateFY2016.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2016.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2016.json", partitions)

      // Count violations by code definition - fy2016
      val violationsByDefinitionFY2016 = timed("Counting violations by violation definition - FY2016", countViolationsByViolationDefinition(violations2016, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2016 = violationsByDefinitionFY2016.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2016, s"${destinationFolder}violation_count_by_violation_definition_fy2016.json", partitions)

      // Count violations per plate type for FY2015
      val violationsByPlateTypeFY2015 = timed("Counting violations by plate type - FY2015", countViolationsByPlateType(violations2015, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2015Sorted = violationsByPlateTypeFY2015.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2015Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2015.json", partitions)

      // Count violations by plate registration for FY2015
      val violationCountByStateFY2015 = timed("Counting violations by registration state - FY2015", countViolationsByState(violations2015, stateDS, sparkSession))
      val sortedViolationCountByStateFY2015 = violationCountByStateFY2015.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2015.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2015.json", partitions)

      // Count violations by code definition - fy2015
      val violationsByDefinitionFY2015 = timed("Counting violations by violation definition - FY2015", countViolationsByViolationDefinition(violations2015, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2015 = violationsByDefinitionFY2015.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2015, s"${destinationFolder}violation_count_by_violation_definition_fy2015.json", partitions)

      // Count violations per plate type for FY2014
      val violationsByPlateTypeFY2014 = timed("Counting violations by plate type - FY2014", countViolationsByPlateType(violations2014, plateTypeDS, sparkSession))
      val violationsByPlateTypeFY2014Sorted = violationsByPlateTypeFY2014.sort(desc("count"))
      saveDataFrameToJson(violationsByPlateTypeFY2014Sorted.toDF(), s"${destinationFolder}violation_by_plate_type_fy2014.json", partitions)

      // Count violations by plate registration for FY2014
      val violationCountByStateFY2014 = timed("Counting violations by registration state - FY2014", countViolationsByState(violations2014, stateDS, sparkSession))
      val sortedViolationCountByStateFY2014 = violationCountByStateFY2014.sort(desc("count"))
      saveDataFrameToJson(sortedViolationCountByStateFY2014.toDF(), s"${destinationFolder}violation_count_by_registration_state_fy2014.json", partitions)

      // Count violations by code definition - fy2014
      val violationsByDefinitionFY2014 = timed("Counting violations by violation definition - FY2014", countViolationsByViolationDefinition(violations2014, violationCodeDS, sparkSession))
      val sortedViolationsByDefinitionFY2014 = violationsByDefinitionFY2014.sort(desc("count"))
      saveDataSetToJson(sortedViolationsByDefinitionFY2014, s"${destinationFolder}violation_count_by_violation_definition_fy2014.json", partitions)

      // Prints the execution times
      println(timing)
    } else {
      println("Missing arguments.")
      logger.error("Missing arguments.")
    }
  }
}
