package org.codecraftlabs.nyc.data

object NYCOpenDataConfig {
  final val ViolationCodeJsonUrl: String = "https://data.cityofnewyork.us/resource/dbw3-ymb4.json"
  final val ParkingViolationsFy2019: String = "https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
  final val ParkingViolationsFy2018: String = "https://data.cityofnewyork.us/resource/9wgk-ev5c.json"
  final val ParkingViolationsFy2017: String = "https://data.cityofnewyork.us/resource/ati4-9cgt.json"
  final val ParkingViolationsFy2016: String = "https://data.cityofnewyork.us/resource/avxe-2nrn.json"
  final val ParkingViolationsFy2015: String = "https://data.cityofnewyork.us/resource/aagd-wyjz.json"
  final val ParkingViolationsFy2014: String = "https://data.cityofnewyork.us/resource/j7ig-zgkq.json"

  final val ConnectionTimeout: Int = 5000
  final val ReadTimeout: Int = 5000
  final val RequestMethod: String = "GET"

}
