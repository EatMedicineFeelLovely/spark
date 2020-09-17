package com.spark.udf.bean

import java.lang.reflect.Method

import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.DataType

import scala.util.Try

class MethodInfo(val instansClass: Any, val method: Method){
  var scalaMethod: AnyRef = null

  /**
    * methods得返回类型
    * @return
    */
  def getParamDTAndReturnDT(): (Option[List[DataType]], DataType) = {
    val rt = method.getReturnType
    val rtDataType = JavaTypeInference.inferDataType(rt)._1
    val paramDataType =
      method.getParameterTypes.map(JavaTypeInference.inferDataType).map(_._1)
    (Try(paramDataType.toList).toOption, rtDataType)
  }

  def getParamsCount(): Int = {
    method.getParameterCount
  }
  /**
    * 运行当前method
    * @param args
    * @tparam T
    * @return
    */
  def call[T](args: Object*): T = {
    method
      .invoke(instansClass, args: _*)
      .asInstanceOf[T]
  }

  override def toString: String = {
    s"""methodName: ${method.getName}, className: ${instansClass.getClass}, scalaFunc: ${scalaMethod}"""
  }
}
