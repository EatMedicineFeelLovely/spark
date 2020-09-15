package com.spark.udf.loader

import java.lang.reflect.Method

import com.spark.udf.bean.{MethodInfo, PreCompileInfo, UDFClassInfo}
import com.spark.udf.core.MethodToScalaFunction
import com.spark.udf.register.UDFRegisterTrait
import scala.tools.reflect.ToolBox
import scala.collection.immutable
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
    */
//  def loadClassForCode(udfClassCodes: Array[(String, String, String)])
//    : Map[String, UDFClassInfo] = {
//    udfClassCodes
//      .filter(x => !hasLoadClass.contains(x._3))
//      .map {
//        case (compileName, code, defualtName) =>
//          val clazz = compile(prepareScala(compileName, code))
//          val methods = clazz.getDeclaredMethods
//          defualtName -> new UDFClassInfo(
//            defualtName,
//            methods
//              .map(m => { m.getName -> new MethodInfo(clazz.newInstance(), m) })
//              .toMap)
//      }
//      .toMap
//  }

//  methods
//  .map(m => {
//    val mthName = m.getName
//    val mInfo = new MethodInfo(clazz, m)
//    mInfo.scalaMethod = MethodToScalaFunction.matchScalaFunc(defualtName,
//      mthName,
//      m.getParameterCount,
//      udfReg)
//    mthName -> mInfo
//  })
//    .toMap
  /**
    * 从代码段里面去加载func，可以是class，也可以是method。class需要指定classname
    * @param udfClassCodes ss
    */
  def getClassInfo(
                    udfClassCodes: Array[PreCompileInfo],
                    methodTran: (Array[Method], Any, String) => Array[MethodInfo])
    : Map[String, UDFClassInfo] = {
    udfClassCodes.map {
      case (info) =>
        hasLoadClass.put(info.classPath, true)
        val clazz = compile(prepareScala(info.compileName, info.code))
        val methods = clazz.getDeclaredMethods
        info.classPath -> new UDFClassInfo(
          info.classPath,
          methodTran(methods, clazz, info.classPath)
            .map(x => (x.method.getName -> x))
            .toMap
        )
    }.toMap
  }

}
