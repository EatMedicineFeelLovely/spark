package com.spark.udf.register.impl

import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 默认从hdfs加载udf jar
  * @param hdsfPath
  */
class HdfsJarUDFRegister(spark: SparkSession, hdsfPath: String = "")
    extends UDFRegister(spark) {
  // udfName -> (object, method)
  lazy val udfMapping = new mutable.HashMap[String, (Any, Method)]

  /**
    *
    * @param udfMapping
    * @param hdsfPath
    */
  def this(spark: SparkSession,
           hdsfPath: String,
           udfMapping: Map[String, String]) {
    this(spark, hdsfPath)
    println(udfMapping, hdsfPath)
    // loadJar
  }

  /**
    *
    */
  private def loadJar(): Unit = {
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    } catch {
      case e: Throwable => println(e.getMessage)
    }
    val url = new URL(hdsfPath)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    // 调取URLClassLoader 的 addURL
    val loaderMethod =
      classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)
  }
}
