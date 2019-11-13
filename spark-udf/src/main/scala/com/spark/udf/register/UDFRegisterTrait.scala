package com.spark.udf.register

import java.io.Serializable
import java.net.{URL, URLClassLoader}

import com.spark.udf.core.UDFClassInfo
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import scala.collection.mutable

trait UDFRegisterTrait extends Serializable {
  // 当前注册器包含得class。防止重复注册
  var loadClassNames: Set[String]

  /**
    * 注册类，实例化，但不写入spark
    * @param _log
    */
  def register()(_log: Logger): Map[String, UDFClassInfo]

  /**
    * 注册进spark
    * @param spark
    * @param _log
    */
  def registerUDF(spark: SparkSession)(
      _log: Logger): Map[String, UDFClassInfo]

  // 同其他得register比较，防止重复注册
  def equalsOtherRegister(obj: Any): Boolean

  def classHashCode(): Int
  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[UDFRegisterTrait])
      equalsOtherRegister(obj)
    else
      false
  }

  override def hashCode(): Int = classHashCode
}
