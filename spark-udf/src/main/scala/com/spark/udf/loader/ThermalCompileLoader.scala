package com.spark.udf.loader

import java.util.UUID

import com.spark.udf.core.MethodToScalaFunction.matchScalaFunc
import com.spark.udf.core.{MethodInfo, UDFClassInfo}
import com.spark.udf.loader.UDFClassLoader.hasLoadClass
import com.spark.udf.register.UDFRegisterTrait

import scala.collection.mutable
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe
object ThermalCompileLoader extends ClassLoaderTrait {
  // 记录加载过得class
  var hasLoadClass: mutable.HashMap[String, Boolean] =
    new mutable.HashMap[String, Boolean]
  lazy val classLoader =
    scala.reflect.runtime.universe.getClass.getClassLoader
  lazy val toolBox = universe.runtimeMirror(classLoader).mkToolBox()

  /**
    *
    * @param src
    * @return
    */
  def compile(src: String): Class[_] = {
    val tree = toolBox.parse(src)
    toolBox.compile(tree).apply().asInstanceOf[Class[_]]
  }

  /**
    * 解决：scala.runtime.BoxedUnit cannot be cast to java.lang.Class
    * @param className
    * @param classBody
    * @return
    */
  def prepareScala(className: String, classBody: String): String = {
    classBody + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }

  /**
    * 从代码段里面去加载func，可以是class，也可以是method。class需要指定classname
    * @param udfClassCodes ss
    */
  def loadClassForCode(udfClassCodes: Array[(String, String, String)])
    : Map[String, UDFClassInfo] = {
    udfClassCodes
      .filter(x => !hasLoadClass.contains(x._3))
      .map {
        case (compileName, code, defualtName) =>
          val clazz = compile(prepareScala(compileName, code))
          val methods = clazz.getDeclaredMethods
          defualtName -> new UDFClassInfo(
            defualtName,
            methods
              .map(m => { m.getName -> new MethodInfo(clazz.newInstance(), m) })
              .toMap)
      }
      .toMap
  }

  /**
    * 从代码段里面去加载func，可以是class，也可以是method。class需要指定classname
    * @param udfClassCodes ss
    */
  def loadClassForCode(udfClassCodes: Array[(String, String, String)],
                       udfReg: UDFRegisterTrait): Map[String, UDFClassInfo] = {
    udfClassCodes
      .map {
        case (compileName, code, defualtName) =>
          hasLoadClass.put(compileName, true)
          val clazz = compile(prepareScala(compileName, code))
          val methods = clazz.getDeclaredMethods
          defualtName -> new UDFClassInfo(
            defualtName,
            methods
              .map(m => {
                val mthName = m.getName
                val mInfo = new MethodInfo(clazz, m)
                mInfo.scalaMethod = matchScalaFunc(defualtName,
                                                   mthName,
                                                   m.getParameterCount,
                                                   udfReg)
                mthName -> mInfo
              })
              .toMap
          )
      }
      .toMap
  }
}
