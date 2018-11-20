package org.codecraftlabs.nyc

import org.apache.log4j.Level.OFF
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, SparkSession}
import org.codecraftlabs.nyc.ParkingViolationsDataHandler.{ColumnNames, readContents}
import org.codecraftlabs.nyc.data.ParkingViolation
import org.codecraftlabs.nyc.utils.DataUtils
import org.codecraftlabs.nyc.utils.DataUtils.saveDataFrameToCsv

object Main {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle NYC Parking violations data set")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
    import sparkSession.implicits._

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

    val violations = removedNullsDF.as[ParkingViolation]
  }
}
