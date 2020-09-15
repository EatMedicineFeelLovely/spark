package com.spark.udf.bean

/**
 *
 * @param className
 * @param methodMap funcName -> Method
 */
class UDFClassInfo(val className: String, val methodMap: Map[String, MethodInfo]) {

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[UDFClassInfo]) {
      obj.asInstanceOf[UDFClassInfo].className == className
    } else false
  }

  override def hashCode(): Int = className.hashCode()

  override def toString: String = {
    s"""[$className: [${methodMap.map(_._1).mkString(",")}]"""
  }

}
