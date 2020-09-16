package com.spark.udf.core

import java.util.UUID

import com.spark.udf.bean.{MethodInfo, PreCompileInfo}
import com.spark.udf.loader.{ClassLoaderTrait, DynamicCompileClassLoader}
import com.spark.udf.register.UDFRegisterTrait

import scala.collection.mutable

object MethodToScalaFunction {
  val methodInfoMap = new mutable.HashMap[String, MethodInfo]()

  /**
    * 匹配scala func。用于注册spark udf
    * 区里面要重新加载。所以这个方法得参数不能是Method，否则报序列话
    * 必须是lazy，clazz 必须在这里创建。。。。否则code的方式报错
    * @return
    */
  def matchScalaFunc(preCompileInfo: PreCompileInfo,
                     @transient classLoader: ClassLoaderTrait,
                     methodName: String,
                     paramCount: Int): AnyRef = {
    lazy val methodInfos = getMethodInfo(preCompileInfo, classLoader, methodName)
    paramCount match {
      case 0 =>
        new (() => Any) with Serializable {
          override def apply(): Any = {
            try {
              methodInfos.call()
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
              methodInfos.call(v1)
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
              methodInfos.call(v1, v2)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }
          }
        }
      case _ => null
    }
  }

  def getMethodInfo(preCompileInfo: PreCompileInfo,
                    @transient classLoader: ClassLoaderTrait,
                    methodName: String): MethodInfo = {

    if (methodInfoMap.contains(s"${preCompileInfo.classPath}.${methodName}")) {
      methodInfoMap(s"${preCompileInfo.classPath}.${methodName}")
    } else {
      val clazz = {
        classLoader.getClassInstance(preCompileInfo)
      }
      val m = new MethodInfo(
        clazz.newInstance(),
        clazz.getDeclaredMethods.filter(_.getName == methodName).head)
      methodInfoMap.put(s"${preCompileInfo.classPath}.${methodName}", m)
      m
    }
  }
}
