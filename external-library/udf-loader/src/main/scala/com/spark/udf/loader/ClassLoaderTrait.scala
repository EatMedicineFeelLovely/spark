package com.spark.udf.loader

import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction

import scala.collection.mutable

trait ClassLoaderTrait extends Serializable {
  @transient var hasLoadClass: mutable.HashMap[String, UDFClassInfo]
  def getClassInfo(
      compileClassInfo: Array[PreCompileInfo]): Map[String, UDFClassInfo]

  def getClassInstance(info: PreCompileInfo): Class[_]

  def getMethodInfo(preCompileInfo: PreCompileInfo,
                    method: Array[Method],
                    clazz: Class[_]): Map[String, MethodInfo] = {
    method
      .map(method => {
        val methodInfo = new MethodInfo(clazz.newInstance(), method)
        methodInfo.scalaMethod = MethodToScalaFunction
          .matchScalaFunc(preCompileInfo, this, method.getName, method.getParameterCount)
        methodInfo
      })
      .map(x => (x.method.getName -> x))
      .toMap

  }

}
