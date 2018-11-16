package org.codecraftlabs.nyc.data

case class ParkingViolation(summonsNumber: Long,
                            plateId: String,
                            registrationState: String,
                            plateType: String,
                            issueDate: String,
                            violationCode: Int,
                            vehicleBodyType: String,
                            vehicleMake: String,
                            issuingAgency: String,
                            vehicleColor: String,
                            violationTime: String,
                            vehicleYear: Int)
