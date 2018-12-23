package org.codecraftlabs.nyc.data

import java.sql.Timestamp

case class ParkingViolationJson()

case class ParkingViolation(summonsNumber: Long,
                            plateId: String,
                            registrationState: String,
                            plateType: String,
                            issueDate: Timestamp,
                            violationCode: Int,
                            vehicleBodyType: String,
                            vehicleMake: String,
                            issuingAgency: String,
                            vehicleColor: String,
                            violationTime: String,
                            vehicleYear: Int,
                            issueDayMonth: Int,
                            issueMonth: Int,
                            issueYear: Int)

case class ByPlateIdCount(plateType: String, count: Long)

case class PlateType(plateType: String, description: String)

case class State(code: String, state: String)

case class OriginalViolationCode (all_other_areas: String, code: String, definition: String, manhattan_96th_st_below: String)

case class ViolationCode (code: Int, definition: String)