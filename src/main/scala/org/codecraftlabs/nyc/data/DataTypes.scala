package org.codecraftlabs.nyc.data

import java.sql.Timestamp

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
                            vehicleYear: Int)

case class ByPlateIdCount(plateType: String, count: Long)