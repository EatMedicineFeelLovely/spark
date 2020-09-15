package com.spark.udf.loader

import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.register.UDFRegisterTrait

import scala.collection.mutable

object UrlClassLoader extends ClassLoaderTrait {
  // 记录加载过得class。可能是不同的加载器，但是是同一个类名，例如两个jar冲突
  var hasLoadClass = new mutable.HashMap[String, Boolean]()

  /**
    * 根据给定得classname 和 func 来读取func
    *  @description 同一个类只加载一次
    * @param classPathMethodName classpath，methodname
    * @return
    */
  override def getClassInfo(
                             classPathMethodName: Array[PreCompileInfo],
                             methodTran: (Array[Method], Any, String) => Array[MethodInfo])
    : Map[String, UDFClassInfo] = {
    classPathMethodName
      .groupBy(_.classPath)
      .filter(x => !hasLoadClass.contains(x._1))
      .map {
        case (classPath, info) =>
          val needMtd = info.map(x => (x.methodNames -> null)).toMap
          hasLoadClass.put(classPath, true)
          val clazz = Class.forName(classPath)
          val methods =
            if (needMtd.contains("_") || needMtd.contains("*")) {
              clazz.getDeclaredMethods
            } else {
              clazz.getDeclaredMethods.filter(m => needMtd.contains(m.getName))
            }
          // val fields = clazz.getDeclaredFields
          classPath -> new UDFClassInfo(
            classPath,
            methodTran(methods, clazz, classPath)
              .map(x => (x.method.getName -> x))
              .toMap
          )
      }

  }
}
