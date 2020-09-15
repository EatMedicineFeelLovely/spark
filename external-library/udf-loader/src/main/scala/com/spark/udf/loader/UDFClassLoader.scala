package com.spark.udf.loader

import com.spark.udf.bean.{MethodInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.register.UDFRegisterTrait

import scala.collection.mutable

object UDFClassLoader extends ClassLoaderTrait {
  // 记录加载过得class。可能是不同的加载器，但是是同一个类名，例如两个jar冲突
  var hasLoadClass = new mutable.HashMap[String, Boolean]()

  /**
    * 根据给定得classname 和 func 来读取func
    * @param udfClassFunc
    * @return
    */
  def classForName(
      udfClassFunc: Array[(String, String)]): Map[String, UDFClassInfo] = {
    udfClassFunc
      .groupBy(_._1)
      .filter(x => !hasLoadClass.contains(x._1))
      .map {
        case (className, funcNamse) =>
          val needMtd = funcNamse.map(x => (x._2 -> null)).toMap
          hasLoadClass.put(className, true)
          val clazz = Class.forName(className)
          val methods =
            if (needMtd.contains("_") || needMtd.contains("*")) {
              clazz.getDeclaredMethods
            } else {
              clazz.getDeclaredMethods.filter(m => needMtd.contains(m.getName))
            }
          // val fields = clazz.getDeclaredFields
          className -> new UDFClassInfo(
            className,
            methods
              .map(m => { m.getName -> new MethodInfo(clazz, m) })
              .toMap)
      }
  }

  /**
    * 根据给定得classname 和 func 来读取func
    * @param udfClassFunc
    * @return
    */
  def classForName(udfClassFunc: Array[(String, String)],
                   udfReg: UDFRegisterTrait): Map[String, UDFClassInfo] = {
    udfClassFunc
      .groupBy(_._1)
      // .filter(x => !hasLoadClass.contains(x._1)) // 如果先做class，再做udf，会导致udf加载不了
      .map {
        case (className, funcNamse) =>
          val needMtd = funcNamse.map(x => (x._2 -> null)).toMap
          hasLoadClass.put(className, true)
          val clazz = Class.forName(className)
          val methods =
            if (needMtd.contains("_") || needMtd.contains("*")) {
              clazz.getDeclaredMethods
            } else {
              clazz.getDeclaredMethods.filter(m => needMtd.contains(m.getName))
            }
          className -> new UDFClassInfo(
            className,
            methods
              .map(m => {
                val mthName = m.getName
                val mInfo = new MethodInfo(clazz, m)
                mInfo.scalaMethod = MethodToScalaFunction.matchScalaFunc(className,
                                                   mthName,
                                                   m.getParameterCount,
                                                   udfReg)
                mthName -> mInfo
              })
              .toMap
          )
      }
  }

}
