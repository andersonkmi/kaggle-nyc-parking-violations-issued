package org.codecraftlabs.nyc.data

import java.sql.Timestamp

case class ParkingViolationJson(date_first_observed: Option[String],
                                days_parking_in_effect: Option[String],
                                feet_from_curb: Option[String],
                                from_hours_in_effect: Option[String],
                                house_number: Option[String],
                                issue_date: Option[String],
                                issuer_code: Option[String],
                                issuer_command: Option[String],
                                issuer_precinct: Option[String],
                                issuer_squad: Option[String],
                                issuing_agency: Option[String],
                                law_section: Option[String],
                                meter_number: Option[String],
                                plate_id: Option[String],
                                plate_type: Option[String],
                                registration_state: Option[String],
                                street_code1: Option[String],
                                street_code2: Option[String],
                                street_code3: Option[String],
                                street_name: Option[String],
                                sub_division: Option[String],
                                summons_number: Option[String],
                                to_hours_in_effect: Option[String],
                                unregistered_vehicle: Option[String],
                                vehicle_body_type: Option[String],
                                vehicle_color: Option[String],
                                vehicle_expiration_date: Option[String],
                                vehicle_make: Option[String],
                                vehicle_year: Option[String],
                                violation_code: Option[String],
                                violation_county: Option[String],
                                violation_in_front_of_or_opposite: Option[String],
                                violation_location: Option[String],
                                violation_precinct: Option[String],
                                violation_time: Option[String])

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
                            violationDescription: String,
                            vehicleYear: Int,
                            issueDayMonth: Int,
                            issueMonth: Int,
                            issueYear: Int)

case class ByPlateTypeCount(plateType: String, count: BigInt)

case class PlateType(plateType: String, description: String)

case class State(code: String, state: String)

case class ViolationCodeJson (all_other_areas: String, code: String, definition: String, manhattan_96th_st_below: String)

case class ViolationCode (code: Int, definition: String)

case class ViolationCountByState(state: String, count: Long)
case class ViolationCountByYear(issueYear: Option[Int], count: Long)
case class ViolationCountByViolationCode(violationCode: Int, count: Long)
case class ViolationCountByViolationDefinition(definition: String, count: Long)