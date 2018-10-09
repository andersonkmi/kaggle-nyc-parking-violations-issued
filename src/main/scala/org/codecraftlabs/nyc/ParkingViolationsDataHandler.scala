package org.codecraftlabs.nyc

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParkingViolationsDataHandler {
  val ColumnNames =
    List("summonsNumber",
    "plateId",
    "registrationState",
    "plateType",
    "issueDate",
    "violationCode",
    "vehicleBodyType",
    "vehicleMake",
    "issuingAgency",
    "streetCode1",
    "streetCode2",
    "streetCode3",
    "vehicleExpirationDate",
    "violationLocation",
    "violationPrecinct",
    "issuerPrecinct",
    "issuerCode",
    "issuerCommand",
    "issuerSquad",
    "violationTime",
    "timeFirstObserved",
    "violationCounty",
    "violationInFrontOfOrOpposite",
    "number",
    "street",
    "intersectingStreet",
    "dateFirstObserved",
    "lawSection",
    "subDivision",
    "violationLegalCode",
    "daysParkingInEffect",
    "fromHoursInEffect",
    "toHoursInEffect",
    "vehicleColor",
    "unregisteredVehicle",
    "vehicleYear",
    "meterNumber",
    "feetFromCurbe",
    "violationPostCode",
    "violationDescription",
    "noStandingOrStoppingViolation",
    "hydrantViolation",
    "doubleParkingViolation")

  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(ColumnNames)).option("header", "true").load(file)
  }

  private def getSchema(colNames: List[String]): StructType = ???
}
