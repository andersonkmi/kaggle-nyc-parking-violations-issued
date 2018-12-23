package org.codecraftlabs.nyc.utils

import org.codecraftlabs.nyc.data.{ParkingViolationJson, ViolationCodeJson}
import org.codecraftlabs.nyc.utils.RestUtils.get
import org.json4s.jackson.JsonMethods.parse

object NYCOpenDataUtils {
  private implicit val formats = org.json4s.DefaultFormats

  def getViolationCodeJsonArray(appToken: String): Array[ViolationCodeJson] = {
    doGet("https://data.cityofnewyork.us/resource/dbw3-ymb4.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ViolationCodeJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2019(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/pvqr-7yc4.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2018(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/9wgk-ev5c.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2017(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/ati4-9cgt.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2016(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/avxe-2nrn.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2015(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/aagd-wyjz.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2014(appToken: String) : Array[ParkingViolationJson] = {
    doGet("https://data.cityofnewyork.us/resource/j7ig-zgkq.json", appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  private def doGet(url: String, appToken: String) : Option[String] = {
    try {
      val content = get(url, appToken)
      Some(content)
    } catch {
      case ioe: java.io.IOException =>  None
      case ste: java.net.SocketTimeoutException => None
    }
  }
}
