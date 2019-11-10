package com.spark.udf.register.common

import java.net.{URL, URLClassLoader}

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

/**
  * 默认从hdfs加载udf jar
  *
  * @param hdsfPath
  */
class UdfRegisterManager(val hdsfPath: String = "") {

  /**
    *
    * @param udfMapping
    * @param hdsfPath
    */
  def this(udfMapping: Map[String, String])(hdsfPath: String = "") {
    this(hdsfPath)
    println(udfMapping, hdsfPath)
  }

  private def loadJar(): Unit = {
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    } catch {
      case e: Throwable => println(e.getMessage)
    }
    val url = new URL(hdsfPath)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod =
      classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)

  }

}
