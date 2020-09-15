package com.spark.udf.loader

import com.spark.udf.bean.{MethodInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.register.UDFRegisterTrait
import scala.tools.reflect.ToolBox
import scala.collection.mutable
import scala.reflect.runtime.universe

object DynamicCompileClassLoader extends ClassLoaderTrait {
  // 记录加载过得class
  var hasLoadClass: mutable.HashMap[String, Boolean] =
    new mutable.HashMap[String, Boolean]
  lazy val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
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
                mInfo.scalaMethod = MethodToScalaFunction.matchScalaFunc(defualtName,
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
