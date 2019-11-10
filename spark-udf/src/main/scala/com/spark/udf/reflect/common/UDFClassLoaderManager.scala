package com.spark.udf.reflect.common

import scala.collection.mutable

class UDFClassLoaderManager {
  private val udfMethodInfos = new mutable.HashMap[String, UDFMethodInfo]

  def this(path: String, udfMapping: Map[String, String]) {
    this()
  }
}
