package org.codecraftlabs.nyc.data

import java.sql.Timestamp

case class ParkingViolationJson(date_first_observed: String,
                                days_parking_in_effect: String,
                                feet_from_curb: String,
                                from_hours_in_effect: String,
                                house_number: String,
                                issue_date: String,
                                issuer_code: String,
                                issuer_command: String,
                                issuer_precinct: String,
                                issuer_squad: String,
                                issuing_agency: String,
                                law_section: String,
                                meter_number: String,
                                plate_id: String,
                                plate_type: String,
                                registration_state: String,
                                street_code1: String,
                                street_code2: String,
                                street_code3: String,
                                street_name: String,
                                sub_division: String,
                                summons_number: String,
                                to_hours_in_effect: String,
                                unregistered_vehicle: String,
                                vehicle_body_type: String,
                                vehicle_color: String,
                                vehicle_expiration_date: String,
                                vehicle_make: String,
                                vehicle_year: String,
                                violation_code: String,
                                violation_county: String,
                                violation_in_front_of_or_opposite: String,
                                violation_location: String,
                                violation_precinct: String,
                                violation_time: String)

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

case class ViolationCodeJson (all_other_areas: String, code: String, definition: String, manhattan_96th_st_below: String)

case class ViolationCode (code: Int, definition: String)