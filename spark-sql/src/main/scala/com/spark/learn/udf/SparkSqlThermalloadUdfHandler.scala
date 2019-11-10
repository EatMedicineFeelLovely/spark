package com.spark.learn.udf

import java.lang.reflect.Method

import com.spark.code.util.ClassFuncReflectUtils
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.DataType

object SparkSqlThermalloadUdfHandler {

  /**
    *获取func得实例， 参数类型，返回类型
    * @return
    */
  def apply(funCode: String): (String, AnyRef, Array[DataType], DataType) = {
    val funcInfo = ClassFuncReflectUtils.createClass(funCode)
    val (argumentTypes, returnType) = getFunctionReturnType(
      funcInfo.methods(""))
    val funcIns =
      generateFunctionx(funCode, argumentTypes.length)
    ("", funcIns, argumentTypes, returnType)
  }

  /**
    * 返回方法得参数类型和返回类型
    * @param med
    * @return
    */
  private def getFunctionReturnType(
      med: Method): (Array[DataType], DataType) = {
    val rt = med.getReturnType
    val rtDataType = JavaTypeInference.inferDataType(rt)._1
    val paramDataType =
      med.getParameterTypes.map(JavaTypeInference.inferDataType).map(_._1)
    (paramDataType, rtDataType)
  }

  /**
    * generateFunctionx(funcCode, med.getParameterTypes.size)
    * 从method中new出一个function
    * @param argumentsNum
    * @return
    */
  def generateFunctionx(funcCode: String, argumentsNum: Int): AnyRef = {
    // lazy 可以解决 序列化问题
    lazy val clazzInfo = ClassFuncReflectUtils.createClass(funcCode)
    lazy val cl = clazzInfo.instance
    lazy val med = clazzInfo.methods("")
    argumentsNum match {
      case 0 =>
        new (() => Any) with Serializable {
          override def apply(): Any = {
            try {
              med.invoke(cl)
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
              med.invoke(cl, v1)
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
              med.invoke(cl, v1, v2)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }
          }
        }

    }
  }

  def main(args: Array[String]): Unit = {
    val f =
      s"""import org.apache.spark.sql.Row
         |def intToRow(str:Int):Row = Row(str*100)""".stripMargin
    val (_, funcIns, argumentTypes, returnType) = SparkSqlThermalloadUdfHandler(
      f)
    println(funcIns, argumentTypes, returnType)
  }
}
