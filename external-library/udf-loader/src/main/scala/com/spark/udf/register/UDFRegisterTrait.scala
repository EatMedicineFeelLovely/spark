package com.spark.udf.register

import java.io.Serializable
import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

trait UDFRegisterTrait extends Serializable {
  // 当前注册器包含得class。防止重复注册
  var loadClassNames: Set[String]
  def getClassInstance(info: PreCompileInfo): Class[_]

  /**
    * 注册类，实例化，但不写入spark
    */
 // def reloadgister()(_log: Logger): Map[String, UDFClassInfo]

  /**
    * 注册进spark
    */
  def registerUDF(): Map[String, UDFClassInfo]

  // 同其他得register比较，防止重复注册
  def equalsOtherRegister(obj: Any): Boolean

  def classHashCode(): Int
  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[UDFRegisterTrait]) {
      equalsOtherRegister(obj)
    } else false
  }

  override def hashCode(): Int = classHashCode

}
