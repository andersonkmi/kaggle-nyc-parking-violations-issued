package org.codecraftlabs.nyc

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.codecraftlabs.nyc.data.{ByPlateTypeCount, ParkingViolation, PlateType}

object DataTransformationUtil {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def getCountByPlateType(ds: Dataset[ParkingViolation], plateTypeDS: Dataset[PlateType], sparkSession: SparkSession): Dataset[ByPlateTypeCount] = {
    import sparkSession.implicits._
    val colNames = Seq("count", "plateType")
    val df = ds.groupBy("plateType").count()
    val plateDF = plateTypeDS.toDF()
    val resultingDF = df.join(plateDF, Seq("plateType"))
    val finalDF = resultingDF.drop("plateType").toDF(colNames: _*)
    finalDF.as[ByPlateTypeCount]
  }

  def filterByYear(ds: Dataset[ParkingViolation], year: Int, sparkSession: SparkSession): Dataset[ParkingViolation] = {
    ds.filter(s"issueYear == $year")
  }
}
