package org.codecraftlabs.nyc

import org.apache.spark.sql.{Dataset, SparkSession}
import org.codecraftlabs.nyc.data.{ByPlateIdCount, ParkingViolation}

object DataTransformationUtil {
  def getCountByPlateType(ds: Dataset[ParkingViolation], sparkSession: SparkSession): Dataset[ByPlateIdCount] = {
    import sparkSession.implicits._
    ds.groupBy("plateType").count().as[ByPlateIdCount]
  }
}
