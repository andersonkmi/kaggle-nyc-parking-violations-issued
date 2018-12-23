package org.codecraftlabs.nyc.utils

import scala.collection.mutable

object ArgsUtils {
  def parseArgs(args: Array[String]): Map[String, String] = {
    val result = mutable.Map[String, String]()

    var currentKey = ""
    for(index <- args.indices) {
      val currentItem = args(index)
      if(currentItem.startsWith("--")) {
        currentKey = currentItem
      } else {
        result(currentKey) = currentItem
      }
    }

    result.toMap
  }
}