package org.codecraftlabs.nyc.utils

import org.apache.spark.sql.Dataset
import org.codecraftlabs.nyc.data.ParkingViolation

object DataUtils {
  def saveDataFrameToCsv(ds: Dataset[ParkingViolation], partitions: Int = 1, destination: String, saveMode: String = "overwrite", header: Boolean = true) : Unit = {
    ds.coalesce(partitions).write.mode(saveMode).option("header", header).csv(destination)
  }
}
