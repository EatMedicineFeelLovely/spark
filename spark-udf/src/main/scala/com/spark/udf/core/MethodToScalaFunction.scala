package com.spark.udf.core

import java.lang.reflect.Method

import com.spark.udf.register.UDFRegisterTrait

object MethodToScalaFunction {

  /**
    * 匹配scala func。用于注册spark udf
    * 区里面要重新加载。所以这个方法得参数不能是Method，否则报序列话
    * @param className
    * @param methodName
    * @param udfReg
    * @return
    */
  def matchScalaFunc(className: String,
                     methodName: String,
                     paramCount: Int,
                     udfReg: UDFRegisterTrait): AnyRef = {
    lazy val classInfo =
      UDFClassLoaderManager().registerClass(udfReg).get(className).get
    lazy val method = classInfo.methodMap(methodName)
    paramCount match {
      case 0 =>
        new (() => Any) with Serializable {
          override def apply(): Any = {
            try {
              method.call()
            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }
          }
        }
      case 1 =>
        new (Object => Any) with Serializable {
          override def apply(v1: Object): Any = {
            try {
              method.call(v1)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }
          }
        }
      case 2 =>
        new ((Object, Object) => Any) with Serializable {
          override def apply(v1: Object, v2: Object): Any = {
            try {
              method.call(v1, v2)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }
          }
        }
    }
  }
}
