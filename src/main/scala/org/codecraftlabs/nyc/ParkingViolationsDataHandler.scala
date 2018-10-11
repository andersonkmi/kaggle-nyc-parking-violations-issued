package org.codecraftlabs.nyc

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParkingViolationsDataHandler {
  private val ColumnNames =
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

  private def getSchema(colNames: List[String]): StructType = {
    val summonsNumberField = StructField(colNames(0), LongType, nullable = false)
    val plateIdField = StructField(colNames(1), StringType, nullable = false)
    val registrationStateField = StructField(colNames(2), StringType, nullable = false)
    val issueDateField = StructField(colNames(3), StringType, nullable = false)
    val violationCodeField = StructField(colNames(4), IntegerType, nullable = false)
    val vehicleBodyTypeField = StructField(colNames(5), StringType, nullable = false)
    val vehicleMakeField = StructField(colNames(6), StringType, nullable = false)
    val issuingAgencyField = StructField(colNames(7), StringType, nullable = false)
    val streetCode1Field = StructField(colNames(8), IntegerType, nullable = false)
    val streetCode2Field = StructField(colNames(9), IntegerType, nullable = false)
    val streetCode3Field = StructField(colNames(10), IntegerType, nullable = false)
    val vehicleExpirationDateField = StructField(colNames(11), LongType, nullable = false)
    val violationLocationField = StructField(colNames(12), StringType, nullable = false)
    val violationPrecinctField = StructField(colNames(13), IntegerType, nullable = false)
    val issuerPrecinctField = StructField(colNames(14), IntegerType, nullable = false)
    val issuerCodeField = StructField(colNames(15), IntegerType, nullable = false)
    val issuerCommandField = StructField(colNames(16), StringType, nullable = false)
    val issuerSquadField = StructField(colNames(17), StringType, nullable = false)
    val violationTimeField = StructField(colNames(18), StringType, nullable = false)
    val timeFirstObservedField = StructField(colNames(19), StringType, nullable = false)
    val violationCountyField = StructField(colNames(20), StringType, nullable = false)
    val violationInFrontOfOrOppositeField = StructField(colNames(21), StringType, nullable = false)
    val numberField = StructField(colNames(22), StringType, nullable = false)
    val streetField = StructField(colNames(23), StringType, nullable = false)
    val intersectingStreetField = StructField(colNames(24), StringType, nullable = false)
    val dateFirstObservedField = StructField(colNames(25), LongType, nullable = false)
    val lawSectionField = StructField(colNames(26), IntegerType, nullable = false)
    val subDivisionField = StructField(colNames(27), StringType, nullable = false)
    val violationLegalCodeField = StructField(colNames(28), StringType, nullable = false)
    val daysParkingInEffectField = StructField(colNames(29), StringType, nullable = false)
    val fromHoursInEffectField = StructField(colNames(30), StringType, nullable = false)
    val toHoursInEffectField = StructField(colNames(31), StringType, nullable = false)
    val vehicleColorField = StructField(colNames(32), StringType, nullable = false)
    val unregisteredVehicleField = StructField(colNames(33), StringType, nullable = false)
    val vehicleYearField = StructField(colNames(34), IntegerType, nullable = false)
    val meterNumberField = StructField(colNames(35), StringType, nullable = false)
    val feetFromCurbeField = StructField(colNames(36), IntegerType, nullable = false)
    val violationPostCodeField = StructField(colNames(37), StringType, nullable = false)
    val violationDescriptionField = StructField(colNames(38), StringType, nullable = false)
    val noStandingOrStoppingViolationField = StructField(colNames(39), StringType, nullable = false)
    val hydrantViolationField = StructField(colNames(40), StringType, nullable = false)
    val doubleParkingViolationField = StructField(colNames(41), StringType, nullable = false)

    val fields = List(
      summonsNumberField,
      plateIdField,
      registrationStateField,
      issueDateField,
      violationCodeField,
      vehicleBodyTypeField,
      vehicleMakeField,
      issuingAgencyField,
      streetCode1Field,
      streetCode2Field,
      streetCode3Field,
      vehicleExpirationDateField,
      violationLocationField,
      violationPrecinctField,
      issuerPrecinctField,
      issuerCodeField,
      issuerCommandField,
      issuerSquadField,
      violationTimeField,
      timeFirstObservedField,
      violationCountyField,
      violationInFrontOfOrOppositeField,
      numberField,
      streetField,
      intersectingStreetField,
      dateFirstObservedField,
      lawSectionField,
      subDivisionField,
      violationLegalCodeField,
      daysParkingInEffectField,
      fromHoursInEffectField,
      toHoursInEffectField,
      vehicleColorField,
      unregisteredVehicleField,
      vehicleYearField,
      meterNumberField,
      feetFromCurbeField,
      violationPostCodeField,
      violationDescriptionField,
      noStandingOrStoppingViolationField,
      hydrantViolationField,
      doubleParkingViolationField)

    StructType(fields)
  }
}
