package org.codecraftlabs.nyc

import org.apache.log4j.Level.OFF
import org.apache.log4j.Logger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.codecraftlabs.nyc.ParkingViolationsDataHandler.{ColumnNames, readContents, readPlatesContent, readStatesContent}
import org.codecraftlabs.nyc.data.{ParkingViolation, PlateType, State}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle NYC Parking violations data set")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val plateTypeDS = readPlatesContent("plates.csv", sparkSession).as[PlateType]
    plateTypeDS.show(100)
    plateTypeDS.printSchema()

    val stateDS = readStatesContent("states.csv", sparkSession).as[State]
    stateDS.show(100)
    stateDS.printSchema()

    val df1 = readContents("parking-violations-issued-fiscal-year-2018.csv", sparkSession)
    val renamedDF = df1.toDF(ColumnNames: _*)

    val df2 = readContents("parking-violations-issued-fiscal-year-2016.csv", sparkSession)
    val renamedDF2 = df2.toDF(ColumnNames: _*)

    val df3 = readContents("parking-violations-issued-fiscal-year-2014.csv", sparkSession)
    val renamedDF3 = df3.toDF(ColumnNames: _*)

    val resultingDF = renamedDF.union(renamedDF2).union(renamedDF3)

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

    val filteredDF = resultingDF.select(df1.columns.filter(colName => colsToKeep.contains(colName)).map(colName => new Column(colName)): _*)
    val removedNullsDF = filteredDF.filter(filteredDF.col("summonsNumber").isNotNull)
    val modifiedDF = removedNullsDF.withColumn("issueDateTemp", unix_timestamp(removedNullsDF.col("issueDate"), "yyyy-MM-dd").cast(TimestampType))
      .drop("issueDate")
      .withColumnRenamed("issueDateTemp", "issueDate")

    val addedCols = modifiedDF
      .withColumn("issueDayMonth", dayofmonth(modifiedDF.col("issueDate")))
      .withColumn("issueMonth", month(modifiedDF.col("issueDate")))
      .withColumn("issueYear", year(modifiedDF.col("issueDate")))
    val violations: Dataset[ParkingViolation] = addedCols.as[ParkingViolation]

    // Split violations by year
    val violations2018 = violations.filter("issueYear == 2018")
    println(violations2018.count())

    val byPlateType = DataTransformationUtil.getCountByPlateType(violations, sparkSession)
    byPlateType.show(100)
  }
}
