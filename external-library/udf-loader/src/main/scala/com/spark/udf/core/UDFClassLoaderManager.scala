package com.spark.udf.core

import java.net.{URL, URLClassLoader}

import com.spark.udf.bean.{MethodInfo, UDFClassInfo}
import com.spark.udf.register.{
  DynamicCompileUDFRegister,
  UDFRegisterTrait,
  UrlJarUDFRegister
}
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object UDFClassLoaderManager {
  val _log = LoggerFactory.getLogger(UDFClassLoaderManager.getClass)
  setURLStreamHandlerFactory
  var udfLoader: UDFClassLoaderManager = null

  /**
    * 单例
    * @return
    */
  def apply(): UDFClassLoaderManager = {
    if (udfLoader == null) {
      udfLoader = new UDFClassLoaderManager()
    }
    udfLoader
  }

  /**
    * 设置 URLStreamHandlerFactory 为hdfs
    * 只能注册一次
    */
  def setURLStreamHandlerFactory(): Unit = {
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    } catch {
      case e: Throwable => e.printStackTrace(); _log.error(e.toString)
    }
  }

  /**
    * 加载hdfs上得jar
    * @param hdsfPaths hdfs:///xxxx/xxx/xxx
    */
  def loadJarFromURL(hdsfPaths: Array[String]): Unit = {
    hdsfPaths.foreach(p => {
      val url = new URL(p)
      val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
      val loaderMethod =
        classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      loaderMethod.setAccessible(true)
      loaderMethod.invoke(classLoader, url)
    })
  }
}
class UDFClassLoaderManager() {
  // 防止某个类重复载入，需要做判断, className -> UDFClassInfo
  val udfClassInfos = new mutable.HashMap[String, UDFClassInfo]
  // key = （className+'.'+methodName） value = MethodInfo 。 如果没用类名责为methodName
  val udfMethodInfos = new mutable.HashMap[String, MethodInfo]
  // 防止重复加载。
  val hasRegistInstans = new mutable.HashMap[UDFRegisterTrait, Boolean]

  def getUdfClassInfo(classPath: String): UDFClassInfo = {
    udfClassInfos(classPath)
  }

  def getMethodInfo(methodPath: String): MethodInfo = {
    udfMethodInfos(methodPath)
  }

  /**
    * 注册类
    * @param udfRegister
    */
  def registerUDF(spark: SparkSession,
                  udfRegister: UDFRegisterTrait*): UDFClassLoaderManager = {
    udfRegister.foreach(r => {
      r match {
        case u: UrlJarUDFRegister =>
          if (!hasRegistInstans.contains(r)) {
            u.registerUDF().foreach {
              case (classPath, lassInfo) =>
                udfClassInfos.put(classPath, lassInfo)
                lassInfo.methodMap.foreach {
                  case (mthName, mtd) =>
                    udfMethodInfos.put(s"$classPath.$mthName", mtd)
                    registerSparkUdf(spark, mtd)
                }
            }
            hasRegistInstans.put(r, true)
          } else {
            UDFClassLoaderManager._log.warn(
              s"  this Register has registed ：  ${r}")
          }
        case dc: DynamicCompileUDFRegister =>
          dc.registerUDF().foreach {
            case (classPath, lassInfo) =>
              udfClassInfos.put(classPath, lassInfo)
              lassInfo.methodMap.foreach {
                case (mthName, mth) =>
                  udfMethodInfos.put(s"$classPath.$mthName", mth)
                  registerSparkUdf(spark, mth)
              }
          }
          hasRegistInstans.put(r, true)
      }
    })
    this
  }
//
//  /**
//   * 注册类
//   * @param udfRegister
//   */
//  def getRegisterClassInfo(udfRegister: UDFRegisterTrait*): Map[String, UDFClassInfo] = {
//    udfRegister.flatMap(r => {
//        r.registerUDF()
//    }).toMap
//  }
  /**
    * 获取某个class得所有method
    * @param className
    */
  def getClass(className: String): UDFClassInfo = {
    udfClassInfos(className)
  }

  /**
    *
    * @param spark
    * @param mInfo
    */
  private def registerSparkUdf(spark: SparkSession, mInfo: MethodInfo): Unit = {
    if (mInfo.scalaMethod != null && spark != null) {
      val mthName = mInfo.method.getName
      if (spark.sessionState.functionRegistry.functionExists(
            new FunctionIdentifier(mthName))) {
        spark.sessionState.functionRegistry
          .dropFunction(new FunctionIdentifier(mthName))
      }
      val (inputTypes, returnType) = mInfo.getParamDTAndReturnDT
      spark.sessionState.functionRegistry
        .registerFunction(
          new FunctionIdentifier(mthName),
          (e: Seq[Expression]) =>
            ScalaUDF(mInfo.scalaMethod,
                     returnType,
                     e,
                     inputTypes
                       .map(_.map(_ => true))
                       .getOrElse(Seq.empty[Boolean]),
                     inputTypes.getOrElse(Nil),
                     Some(mthName))
        )
    } else {
      println("scala func is Match fail" )
    }

  }

  /**
    * func 可能重复，指定到某个固定得class
    * @param funcName
    */
  def getUDF(className: String, funcName: String): MethodInfo = {
    udfMethodInfos(s"$className.$funcName")
  }

  /**
    * func 不会重复
    * @param funcName
    */
  def getUDF(funcName: String): MethodInfo = {
    getUDF("defualt", funcName)
  }
}
