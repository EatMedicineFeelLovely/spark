package com.spark.udf.register

import java.lang.reflect.Method
import java.util.UUID

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.loader.{DynamicCompileClassLoader}
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import scala.collection.JavaConverters._

/**
  *
  * @param classNameCodesStr 代码段，1：类名 2：code代码，如果只是func，classname可以是空
  */
class DynamicCompileUDFRegister(val classNameCodesStr: Array[(String, String)])
    extends UDFRegisterTrait {
  var loadClassNames: Set[String] = _
  // 修正udfcodes
  var fixPrecInfos = classNameCodesStr.map {
    case (className, code) =>
      if (className == null || className.isEmpty) {
        val fixClassname =
          s"""class_${UUID.randomUUID().toString.replaceAll("-", "")}"""
        PreCompileInfo("defualt",
                       fixClassname,
                       null,
                       s"""class $fixClassname{
             | $code
             |}""".stripMargin,
        )
      } else PreCompileInfo(className, className, null, code)
  }

  /**
    *    * @return
    */
  override def registerUDF(): Map[String, UDFClassInfo] = {
    DynamicCompileClassLoader.getClassInfo( fixPrecInfos )
  }

  /**
    * 比较两个是否为同一个
    * @param obj
    * @return
    */
  override def equalsOtherRegister(obj: Any): Boolean = {
    if (obj.isInstanceOf[DynamicCompileUDFRegister]) {
      val other = obj.asInstanceOf[DynamicCompileUDFRegister]
      other.classNameCodesStr.equals(classNameCodesStr)
    } else false
  }

  /**
    * 用于比较两个bean是否相等。解决重复加载问题
    * @return
    */
  override def classHashCode(): Int = {
    classNameCodesStr.hashCode()
  }
  override def getClassInstance(info: PreCompileInfo): Class[_] =
    DynamicCompileClassLoader.getClassInstance(info)

  override def toString: String =
    s"""ThermalCompileUDFRegister : [${fixPrecInfos
      .map(x => (x.classPath, x.code))
      .mkString(",")}]"""
}
