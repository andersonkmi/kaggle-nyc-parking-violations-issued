package org.codecraftlabs.nyc.utils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.codecraftlabs.nyc.data._

object DataTransformationUtil {
  def countViolationsByPlateType(ds: Dataset[ParkingViolation], plateTypeDS: Dataset[PlateType], sparkSession: SparkSession): Dataset[ByPlateTypeCount] = {
    import sparkSession.implicits._
    val joinedDS = ds.join(plateTypeDS, Seq("plateType"))
    val df = joinedDS.groupBy("description").count()
    val renamedDF = df.toDF(Seq("plateType", "count"): _*)
    renamedDF.as[ByPlateTypeCount]
  }

  def countViolationsByState(ds: Dataset[ParkingViolation], stateDS: Dataset[State], sparkSession: SparkSession): Dataset[ViolationCountByState] = {
    import sparkSession.implicits._
    val joinDS = ds.join(stateDS, ds.col("registrationState") === stateDS.col("code"), "inner")
    val df = joinDS.groupBy("state").count()
    df.as[ViolationCountByState]
  }

  def countViolationsByYear(ds: Dataset[ParkingViolation], sparkSession: SparkSession): Dataset[ViolationCountByYear] = {
    import sparkSession.implicits._
    val df = ds.groupBy("issueYear").count()
    df.as[ViolationCountByYear]
  }

  def countViolationsByViolationCode(ds: Dataset[ParkingViolation], sparkSession: SparkSession): Dataset[ViolationCountByViolationCode] = {
    import sparkSession.implicits._
    val df = ds.groupBy("violationCode").count()
    df.as[ViolationCountByViolationCode]
  }

  def countViolationsByViolationDefinition(ds: Dataset[ParkingViolation], violationDS: Dataset[ViolationCode], sparkSession: SparkSession): Dataset[ViolationCountByViolationDefinition] = {
    import sparkSession.implicits._
    val joinDS = ds.join(violationDS, ds.col("violationCode") === violationDS.col("code"))
    val df = joinDS.groupBy("definition").count()
    val renamedDF = df.toDF(Seq("definition", "count"): _*)
    renamedDF.as[ViolationCountByViolationDefinition]
  }

  def filterByYear(ds: Dataset[ParkingViolation], year: Int, sparkSession: SparkSession): Dataset[ParkingViolation] = {
    ds.filter(s"issueYear == $year")
  }

  def filterByYears(ds: Dataset[ParkingViolation], years: List[Int], sparkSession: SparkSession): Dataset[ParkingViolation] = {
    ds.filter(ds.col("issueYear").isin(years).desc("issueYear"))
  }
}
