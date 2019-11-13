package com.spark.udf.register

import java.util.UUID

import com.spark.udf.core.UDFClassInfo
import com.spark.udf.loader.ThermalCompileLoader
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import scala.collection.mutable

/**
  *
  * @param udfClassCodes 代码段，1：类名 2：code代码，如果是func，class可以是空
  */
class ThermalCompileUDFRegister(val udfClassCodes: Array[(String, String)])
    extends UDFRegisterTrait {
  var loadClassNames: Set[String] = _
  // 修正udfcodes
  var fixUdfConde = udfClassCodes.map {
    case (className, code) =>
      if (className == null || className.isEmpty) {
        val fixClassname =
          s"""class_${UUID.randomUUID().toString.replaceAll("-", "")}"""
        (fixClassname,
         s"""class $fixClassname{
             | $code
             |}""".stripMargin,
         "defualt")
      } else (className, code, className)
  }
  // 不注册类暂时
  override def register()(_log: Logger): Map[String, UDFClassInfo] = {
    ThermalCompileLoader.loadClassForCode(fixUdfConde)
  }

  /**
    *
    * @param spark
    * @param _log
    * @return
    */
  override def registerUDF(spark: SparkSession)(
      _log: Logger): Map[String, UDFClassInfo] = {
    ThermalCompileLoader.loadClassForCode(fixUdfConde, this)
  }

  /**
    * 比较两个是否为同一个
    * @param obj
    * @return
    */
  override def equalsOtherRegister(obj: Any): Boolean = {
    if (obj.isInstanceOf[ThermalCompileUDFRegister]) {
      val other = obj.asInstanceOf[ThermalCompileUDFRegister]
      other.udfClassCodes.equals(udfClassCodes)
    } else false
  }

  /**
    * 用于比较两个bean是否相等。解决重复加载问题
    * @return
    */
  override def classHashCode(): Int = {
    udfClassCodes.hashCode()
  }
  override def toString: String =
    s"""ThermalCompileUDFRegister : [${fixUdfConde
      .map(x => (x._3, x._2))
      .mkString(",")}]"""
}
