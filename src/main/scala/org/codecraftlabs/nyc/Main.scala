package org.codecraftlabs.nyc

import org.apache.log4j.Level.OFF
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, SparkSession}
import org.codecraftlabs.nyc.ParkingViolationsDataHandler.readContents
import org.codecraftlabs.nyc.data.ParkingViolation

object Main {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle NYC Parking violations data set")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val df = readContents("parking-violations-issued-fiscal-year-2018.csv", sparkSession)

    val renamedDF = df.toDF(ParkingViolationsDataHandler.ColumnNames: _*)

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

    val filteredDF = renamedDF.select(df.columns .filter(colName => colsToKeep.contains(colName)) .map(colName => new Column(colName)): _*)
    val removedNullsDF = filteredDF.filter(filteredDF.col("summonsNumber").isNotNull)

    val violations = removedNullsDF.as[ParkingViolation]
    violations.show(200)
  }
}
