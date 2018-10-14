package org.codecraftlabs.nyc

import org.apache.log4j.Level.OFF
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle NYC Parking violations data set")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-nyc-parking-violations").master("local[*]").getOrCreate()

    val df = ParkingViolationsDataHandler.readContents("parking-violations-issued-fiscal-year-2018.csv", sparkSession)
    val renamedDF = df.toDF(ParkingViolationsDataHandler.ColumnNames: _*)
    renamedDF.printSchema()
    renamedDF.show(20)
  }
}
