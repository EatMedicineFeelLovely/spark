//package com.spark.udf.register
//
//import org.apache.spark.sql.SparkSession
//import java.lang.reflect.Method
//import java.net.{URL, URLClassLoader}
//
//import com.spark.udf.core.{UDFClassInfo, UDFClassLoaderManager}
//import com.spark.udf.loader.UDFClassLoader
//import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
//import org.slf4j.Logger
//
//import scala.collection.mutable
//
///**
//  * @param udfClassFunc 类名，方法名
//  * @param hdsfPaths
//  */
//class HdfsJarUDFRegister(val hdsfPaths: Array[String],
//                         val udfClassFunc: Array[(String, String)])
//    extends UDFRegisterTrait {
//  var loadClassNames: Set[String] = udfClassFunc.map(_._1).toSet
//
//  /**
//    * 注册class和func。返回一个 Map[className, classInfo]。会做防重复加载
//    * @param _log
//    */
//  override def register()(_log: Logger): Map[String, UDFClassInfo] = {
//    UDFClassLoaderManager.loadJarFromURL(hdsfPaths)
//    UDFClassLoader.classForName(udfClassFunc)
//  }
//
//  /**
//    * 将func注册进spark
//    * @param spark
//    * @param _log
//    */
//  override def registerUDF(spark: SparkSession, classInfoMap: mutable.HashMap[String, UDFClassInfo])(
//      _log: Logger): Array[AnyRef] = ???
//
//  /**
//    * 比较两个是否为同一个
//    * @param obj
//    * @return
//    */
//  override def equalsOtherRegister(obj: Any): Boolean = {
//    if (obj.isInstanceOf[HdfsJarUDFRegister]) {
//      val other = obj.asInstanceOf[HdfsJarUDFRegister]
//      other.hdsfPaths.equals(hdsfPaths) && udfClassFunc.equals(
//        other.udfClassFunc)
//    } else false
//  }
//  override def classHashCode(): Int = {
//    udfClassFunc.hashCode()
//  }
//}
