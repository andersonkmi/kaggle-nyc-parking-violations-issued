package org.codecraftlabs.nyc.utils

import org.codecraftlabs.nyc.data.NYCOpenDataConfig._
import org.codecraftlabs.nyc.data.{ParkingViolationJson, ViolationCodeJson}
import org.codecraftlabs.spark.utils.RestUtils.get
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object NYCOpenDataUtils {
  private implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def getViolationCodeJsonArray(appToken: String): Array[ViolationCodeJson] = {
    doGet(ViolationCodeJsonUrl, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ViolationCodeJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2019(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2019, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2018(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2018, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2017(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2017, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2016(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2016, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2015(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2015, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  def getParkingViolationsFiscalYear2014(appToken: String) : Array[ParkingViolationJson] = {
    doGet(ParkingViolationsFy2014, appToken) match {
      case Some(contents) => parse(contents).extract[Array[ParkingViolationJson]]
      case None => Array()
    }
  }

  private def doGet(url: String, appToken: String) : Option[String] = {
    try {
      val headers = Map("X-App-Token" -> appToken)
      val content = get(url, ConnectionTimeout, ReadTimeout, RequestMethod, headers)
      Some(content)
    } catch {
      case _: java.io.IOException =>  None
      case _: java.net.SocketTimeoutException => None
    }
  }
}
