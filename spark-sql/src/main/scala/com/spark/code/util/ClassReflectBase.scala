package com.spark.code.util

import java.util
import java.lang.reflect.{Field, Method}
import java.util
import java.util.UUID

import com.spark.code.util.ClassFuncReflectUtils.ClassInfo
import org.apache.spark.sql.Row

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

trait ClassReflectBase extends Serializable {
  lazy val clazzMap = new util.HashMap[String, ClassInfo]()
  lazy val funcMap = new util.HashMap[String, FuncInfo]()
  lazy val classLoader =
    scala.reflect.runtime.universe.getClass.getClassLoader
  lazy val toolBox = universe.runtimeMirror(classLoader).mkToolBox()

  def compile(src: String): Class[_] = {
    val tree = toolBox.parse(src)
    toolBox.compile(tree).apply().asInstanceOf[Class[_]]
  }
  def prepareScala(className: String, classBody: String): String = {
    classBody + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }

  /**
    * 可以同时写多个方法。根据不同得方法注册不同得udf
    * @param clazz
    * @param instance
    * @param methods
    * @param fields
    */
  case class ClassInfo(clazz: Class[_],
                       instance: Any,
                       methods: Map[String, Method],
                       fields: Array[Field])
  case class FuncInfo(funcName: String, instance: Any, method: Method) {
    def call[T](args: Object*) = {
      method
        .invoke(instance, args: _*)
        .asInstanceOf[T]
    }
  }
}
