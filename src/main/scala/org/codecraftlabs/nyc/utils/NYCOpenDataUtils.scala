package org.codecraftlabs.nyc.utils

import org.codecraftlabs.nyc.data.OriginalViolationCode
import org.json4s.jackson.JsonMethods.parse

object NYCOpenDataUtils {
  private implicit val formats = org.json4s.DefaultFormats

  def getViolationCodeJsonArray(): Array[OriginalViolationCode] = {
    doGet("https://data.cityofnewyork.us/resource/dbw3-ymb4.json") match {
      case Some(contents) => parse(contents).extract[Array[OriginalViolationCode]]
      case None => Array()
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
