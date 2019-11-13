package com.spark.udf.core

import java.net.{URL, URLClassLoader}

import com.spark.udf.register.UDFRegisterTrait
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import MethodToScalaFunction._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

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
    * @param hdsfPaths
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
  // 防止某个类重复载入，需要做判断
  private val udfClassInfos = new mutable.HashMap[String, UDFClassInfo]
  // key = （className+'.'+methodName） value = MethodInfo 。 如果没用类名责为methodName
  private val udfMethodInfos = new mutable.HashMap[String, MethodInfo]
  // 防止重复加载。
  private val hasRegistInstans = new mutable.HashMap[UDFRegisterTrait, Boolean]

  /**
    * 注册类。这个也只有hdfsJarUDFRegister会用
    * @param udfRegister
    */
  def registerClass(
      udfRegister: UDFRegisterTrait*): mutable.HashMap[String, UDFClassInfo] = {
    udfRegister.foreach(r => {
      if (!hasRegistInstans.contains(r)) {
        r.register()(UDFClassLoaderManager._log).foreach {
          case (className, lassInfo) =>
            udfClassInfos.put(className, lassInfo)
            lassInfo.methodMap.foreach {
              case (mthName, mtd) =>
                udfMethodInfos.put(s"$className.$mthName", mtd)
            }
        }
        hasRegistInstans.put(r, true)
      } else
        UDFClassLoaderManager._log.warn(s"  this Register has registed ：  ${r}")
    })
    udfClassInfos
  }

  /**
    * 注册udf
    * @param spark
    * @param udfRegister
    */
  def registerUDF(
      spark: SparkSession,
      udfRegister: UDFRegisterTrait*): mutable.HashMap[String, MethodInfo] = {
    udfRegister.foreach(r => {
     // if (!hasRegistInstans.contains(r)) {
        r.registerUDF(spark)(UDFClassLoaderManager._log).foreach {
          case (className, lassInfo) =>
            udfClassInfos.put(className, lassInfo)
            lassInfo.methodMap.foreach {
              case (mthName, mth) =>
                udfMethodInfos.put(s"$className.$mthName", mth)
                if (spark.sessionState.functionRegistry.functionExists(
                      new FunctionIdentifier(mthName)))
                  spark.sessionState.functionRegistry
                    .dropFunction(new FunctionIdentifier(mthName))
                val (inputTypes, returnType) = mth.getParamDTAndReturnDT
                spark.sessionState.functionRegistry
                  .registerFunction(
                    new FunctionIdentifier(mthName),
                    (e: Seq[Expression]) =>
                      ScalaUDF(mth.scalaMethod,
                               returnType,
                               e,
                               inputTypes
                                 .map(_.map(_ => true))
                                 .getOrElse(Seq.empty[Boolean]),
                               inputTypes.getOrElse(Nil),
                               Some(mthName))
                  )
            }
        }
        hasRegistInstans.put(r, true)
//      } else
//        UDFClassLoaderManager._log.warn(s"  this Register has registed ：  ${r}")
    })
    udfMethodInfos
  }

  /**
    * 获取某个class得所有method
    * @param className
    */
  def getClass(className: String): UDFClassInfo = {
    udfClassInfos(className)
  }

  /**
    * func 可能重复，指定到某个固定得class
    * @param funcName
    */
  def getUDF(className: String, funcName: String): MethodInfo = {
    getUDF(s"$className.$funcName")
  }

  /**
    * func 不会重复
    * @param funcName
    */
  def getUDF(funcName: String): MethodInfo = {
    udfMethodInfos(funcName)
  }
}
