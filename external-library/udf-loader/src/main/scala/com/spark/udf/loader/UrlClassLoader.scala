package com.spark.udf.loader

import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.register.UDFRegisterTrait

import scala.collection.mutable

object UrlClassLoader extends ClassLoaderTrait {
  // 记录加载过得class。可能是不同的加载器，但是是同一个类名，例如两个jar冲突
  var hasLoadClass = new mutable.HashMap[String, UDFClassInfo]()

  /**
    * 根据给定得classname 和 func 来读取func
    *  @description 同一个类只加载一次
    * @param classPathMethodName classpath，methodname
    * @return
    */
  override def getClassInfo(
      classPathMethodName: Array[PreCompileInfo]): Map[String, UDFClassInfo] = {
    classPathMethodName
      .groupBy(_.classPath)
      .map {
        case (classPath, info) =>
          if (hasLoadClass.contains(classPath)) {
            classPath -> hasLoadClass(classPath)
          } else {
            val preCompileInfo = PreCompileInfo(classPath, null, null, null)
            val needMtd = info.map(x => (x.methodNames -> null)).toMap
            val clazz =
              getClassInstance(preCompileInfo)
            val methods =
              (if (needMtd.contains("_") || needMtd.contains("*")) {
                 clazz.getDeclaredMethods
               } else {
                 clazz.getDeclaredMethods.filter(m =>
                   needMtd.contains(m.getName))
               })

            val classInfo =
              new UDFClassInfo(classPath, getMethodInfo(preCompileInfo, methods, clazz))
            hasLoadClass.put(classPath, classInfo)
            // val fields = clazz.getDeclaredFields
            classPath -> classInfo
          }
      }
  }

  /**
    *
    * @param info
    * @return
    */
  override def getClassInstance(info: PreCompileInfo): Class[_] = {
    Class.forName(info.classPath)
  }
}
