package com.spark.udf.register

import java.io.Serializable
import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

trait UDFRegisterTrait extends Serializable {
  // 当前注册器包含得class。防止重复注册
  var loadClassNames: Set[String]

  /**
    * 注册类，实例化，但不写入spark
    */
 // def reloadgister()(_log: Logger): Map[String, UDFClassInfo]

  /**
    * 注册进spark
    */
  def registerUDF(isRegisterUdf: Boolean = true): Map[String, UDFClassInfo]

  // 同其他得register比较，防止重复注册
  def equalsOtherRegister(obj: Any): Boolean

  def classHashCode(): Int
  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[UDFRegisterTrait]) {
      equalsOtherRegister(obj)
    } else false
  }

  override def hashCode(): Int = classHashCode


  /**
   * 将方法转为scala udf
   * @param u
   * @return
   */
  def transMethodToScalaFunc(u: UDFRegisterTrait)
  : (Array[Method], Any, String) => Array[MethodInfo] =
    (methods: Array[Method], clazz: Any, className: String) =>
      methods
        .map(m => {
          val mthName = m.getName
          val mInfo = new MethodInfo(clazz, m)
          mInfo.scalaMethod = MethodToScalaFunction
            .matchScalaFunc(className, mthName, m.getParameterCount, u)
          mInfo
        })


  /**
   *
   * @return
   */
  def transMethodToInfo(): (Array[Method], Any, String) => Array[MethodInfo] = {
    (methods: Array[Method], clazz: Any, className: String) =>
      methods
        .map(m => { new MethodInfo(clazz, m) })
  }
}
