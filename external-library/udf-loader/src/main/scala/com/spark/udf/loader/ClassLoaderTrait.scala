package com.spark.udf.loader

import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}

import scala.collection.mutable

trait ClassLoaderTrait {
  var hasLoadClass: mutable.HashMap[String, Boolean]
  def getClassInfo(
      compileClassInfo: Array[PreCompileInfo],
      methodTran: (Array[Method], Any, String) => Array[MethodInfo])
    : Map[String, UDFClassInfo]
}
