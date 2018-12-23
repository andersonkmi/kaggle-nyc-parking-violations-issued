package org.codecraftlabs.nyc.utils

import org.codecraftlabs.nyc.data.ViolationCode
import org.json4s.jackson.JsonMethods.parse

object NYCOpenDataUtils {
  private implicit val formats = org.json4s.DefaultFormats

  def getViolationCodeJsonArray(): Option[Array[ViolationCode]] = {
    doGet("https://data.cityofnewyork.us/resource/dbw3-ymb4.json") match {
      case Some(contents) => Some(parse(contents).extract[Array[ViolationCode]])
      case None => None
    }
  }

  private def doGet(url: String) : Option[String] = {
    try {
      val content = RestUtils.get(url)
      Some(content)
    } catch {
      case ioe: java.io.IOException =>  None
      case ste: java.net.SocketTimeoutException => None
    }
  }
}
