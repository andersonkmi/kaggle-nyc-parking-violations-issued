package org.codecraftlabs.nyc

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.codecraftlabs.nyc.data.{ByPlateTypeCount, ParkingViolation, PlateType}

object DataTransformationUtil {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def getCountByPlateType(ds: Dataset[ParkingViolation], plateTypeDS: Dataset[PlateType], sparkSession: SparkSession): Dataset[ByPlateTypeCount] = {
    import sparkSession.implicits._
    val joinedDS = ds.join(plateTypeDS, Seq("plateType"))
    val df = joinedDS.groupBy("description").count()
    val renamedDF = df.toDF(Seq("plateType", "count"): _*)
    renamedDF.as[ByPlateTypeCount]
  }

  def filterByYear(ds: Dataset[ParkingViolation], year: Int, sparkSession: SparkSession): Dataset[ParkingViolation] = {
    ds.filter(s"issueYear == $year")
  }

  def filterByYears(ds: Dataset[ParkingViolation], startYear: Int, endYear: Int, sparkSession: SparkSession): Dataset[ParkingViolation] = {
    ds.filter(ds.col("issueYear").geq(startYear)).filter(ds.col("issueYear").leq(endYear))
  }
}
