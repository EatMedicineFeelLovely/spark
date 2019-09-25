package com.spark.code.util

import java.lang.reflect.Method
import java.util
import java.util.UUID

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
object ClassCreateUtils {

  /**
    * 动态地创建一个class。通过string代码
    */
  private val clazzs = new util.HashMap[String, ClassInfo]()
  private val classLoader =
    scala.reflect.runtime.universe.getClass.getClassLoader
  private val toolBox = universe.runtimeMirror(classLoader).mkToolBox()

  /**
    * 创建一个带func 地class
    * @param func
    * @return
    */
  def apply(func: String): ClassInfo = this.synchronized {
    var clazz = clazzs.get(func)
    if (clazz == null) {
      val (className, classBody) = wrapClass(func)
      val zz = compile(prepareScala(className, classBody))
      val defaultMethod = zz.getDeclaredMethods.head // 第一个public func
      val methods = zz.getDeclaredMethods // 所有地func
      clazz = ClassInfo(
        zz,
        zz.newInstance(),
        defaultMethod,
        methods = methods.map { m =>
          (m.getName, m)
        }.toMap,
        func
      )
      clazzs.put(func, clazz)
    }
    clazz
  }
  def compile(src: String): Class[_] = {
    val tree = toolBox.parse(src)
    toolBox.compile(tree).apply().asInstanceOf[Class[_]]
  }
  def prepareScala(className: String, classBody: String): String = {
    classBody + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }
  def wrapClass(function: String): (String, String) = {
    val className =
      s"dynamic_class_${UUID.randomUUID().toString.replaceAll("-", "")}"
    val classBody =
      s"""
         |class $className{
         |  $function
         |}
            """.stripMargin
    (className, classBody)
  }

  case class ClassInfo(clazz: Class[_],
                       instance: Any,
                       defaultMethod: Method,
                       methods: Map[String, Method],
                       func: String) {
    def defaultMethodinvoke[T](args: Object*): T = {
      defaultMethod.invoke(instance, args: _*).asInstanceOf[T]
    }
  }

  def main(args: Array[String]): Unit = {
    val classFunc = ClassCreateUtils(s"def myUDFtoUps(str:Int):Int = str*100")
    val myMethod = classFunc.methods("myUDFtoUps")
    println(myMethod.invoke(classFunc.instance, new Integer(1))) // method1
    // println(classFunc.defaultMethodinvoke(new Integer(1))) // 默认地func
  }
}
