package org.codecraftlabs.nyc

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParkingViolationsDataHandler {
  val Columns = Seq("Summons Number",
    "Plate ID",
    "Registration State",
    "Plate Type",
    "Issue Date",
    "Violation Code",
    "Vehicle Body Type",
    "Vehicle Make",
    "Issuing Agency",
    "Street Code1",
    "Street Code2",
    "Street Code3",
    "Vehicle Expiration Date",
    "Violation Location",
    "Violation Precinct",
    "Issuer Precinct",
    "Issuer Code",
    "Issuer Command",
    "Issuer Squad",
    "Violation Time",
    "Time First Observed",
    "Violation County",
    "Violation In Front Of Or Opposite",
    "House Number",
    "Street Name",
    "Intersecting Street",
    "Date First Observed",
    "Law Section",
    "Sub Division",
    "Violation Legal Code",
    "Days Parking In Effect    ",
    "From Hours In Effect",
    "To Hours In Effect",
    "Vehicle Color",
    "Unregistered Vehicle?",
    "Vehicle Year",
    "Meter Number",
    "Feet From Curb",
    "Violation Post Code",
    "Violation Description",
    "No Standing or Stopping Violation",
    "Hydrant Violation",
    "Double Parking Violation")

  val ColumnNames =
    Seq(
    "summonsNumber",
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
    "houseNumber",
    "streetName",
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
    "feetFromCurb",
    "violationPostCode",
    "violationDescription",
    "noStandingOrStoppingViolation",
    "hydrantViolation",
    "doubleParkingViolation")

  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(Columns.toList)).option("header", "true").option("delimiter", ",").load(file)
  }

  private def getSchema(colNames: List[String]): StructType = {
    val summonsNumberField = StructField(colNames.head, LongType, nullable = true)
    val plateIdField = StructField(colNames(1), StringType, nullable = true)
    val registrationStateField = StructField(colNames(2), StringType, nullable = true)
    val plateTypeField = StructField(colNames(3), StringType, nullable = true)
    val issueDateField = StructField(colNames(4), StringType, nullable = true)
    val violationCodeField = StructField(colNames(5), IntegerType, nullable = true)
    val vehicleBodyTypeField = StructField(colNames(6), StringType, nullable = true)
    val vehicleMakeField = StructField(colNames(7), StringType, nullable = true)
    val issuingAgencyField = StructField(colNames(8), StringType, nullable = true)
    val streetCode1Field = StructField(colNames(9), IntegerType, nullable = true)
    val streetCode2Field = StructField(colNames(10), IntegerType, nullable = true)
    val streetCode3Field = StructField(colNames(11), IntegerType, nullable = true)
    val vehicleExpirationDateField = StructField(colNames(12), LongType, nullable = true)
    val violationLocationField = StructField(colNames(13), StringType, nullable = true)
    val violationPrecinctField = StructField(colNames(14), IntegerType, nullable = true)
    val issuerPrecinctField = StructField(colNames(15), IntegerType, nullable = true)
    val issuerCodeField = StructField(colNames(16), IntegerType, nullable = true)
    val issuerCommandField = StructField(colNames(17), StringType, nullable = true)
    val issuerSquadField = StructField(colNames(18), StringType, nullable = true)
    val violationTimeField = StructField(colNames(19), StringType, nullable = true)
    val timeFirstObservedField = StructField(colNames(20), StringType, nullable = true)
    val violationCountyField = StructField(colNames(21), StringType, nullable = true)
    val violationInFrontOfOrOppositeField = StructField(colNames(22), StringType, nullable = true)
    val numberField = StructField(colNames(23), StringType, nullable = true)
    val streetField = StructField(colNames(24), StringType, nullable = true)
    val intersectingStreetField = StructField(colNames(25), StringType, nullable = true)
    val dateFirstObservedField = StructField(colNames(26), LongType, nullable = true)
    val lawSectionField = StructField(colNames(27), IntegerType, nullable = true)
    val subDivisionField = StructField(colNames(28), StringType, nullable = true)
    val violationLegalCodeField = StructField(colNames(29), StringType, nullable = true)
    val daysParkingInEffectField = StructField(colNames(30), StringType, nullable = true)
    val fromHoursInEffectField = StructField(colNames(31), StringType, nullable = true)
    val toHoursInEffectField = StructField(colNames(32), StringType, nullable = true)
    val vehicleColorField = StructField(colNames(33), StringType, nullable = true)
    val unregisteredVehicleField = StructField(colNames(34), StringType, nullable = true)
    val vehicleYearField = StructField(colNames(35), IntegerType, nullable = true)
    val meterNumberField = StructField(colNames(36), StringType, nullable = true)
    val feetFromCurbField = StructField(colNames(37), IntegerType, nullable = true)
    val violationPostCodeField = StructField(colNames(38), StringType, nullable = true)
    val violationDescriptionField = StructField(colNames(39), StringType, nullable = true)
    val noStandingOrStoppingViolationField = StructField(colNames(40), StringType, nullable = true)
    val hydrantViolationField = StructField(colNames(41), StringType, nullable = true)
    val doubleParkingViolationField = StructField(colNames(42), StringType, nullable = true)

    val fields = List(
      summonsNumberField,
      plateIdField,
      registrationStateField,
      plateTypeField,
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
      feetFromCurbField,
      violationPostCodeField,
      violationDescriptionField,
      noStandingOrStoppingViolationField,
      hydrantViolationField,
      doubleParkingViolationField)

    StructType(fields)
  }

  def readPlatesContent(file: String, session: SparkSession): DataFrame = {
    val cols = List("plateType", "description")
    session.read.format("com.databricks.spark.csv").schema(getPlatesSchema(cols)).option("header", "true").option("delimiter", ",").load(file)
  }

  private def getPlatesSchema(columnNames: List[String]): StructType = {
    val plateType = StructField(columnNames.head, StringType, nullable = true)
    val description = StructField(columnNames(1), StringType, nullable = true)

    val fieldList = List(plateType, description)
    StructType(fieldList)
  }

  def readStatesContent(file: String, session: SparkSession): DataFrame = {
    val cols = List("code", "state")
    session.read.format("com.databricks.spark.csv").schema(getStatesSchema(cols)).option("header", "true").option("delimiter", ",").load(file)
  }

  private def getStatesSchema(columnNames: List[String]): StructType = {
    val code = StructField(columnNames.head, StringType, nullable = true)
    val state = StructField(columnNames(1), StringType, nullable = true)

    val fieldList = List(code, state)
    StructType(fieldList)
  }

}
