package com.spark.code.util

import java.lang.reflect.{Field, Method}
import java.util
import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.JavaTypeInference

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
object ClassFuncReflectUtils extends ClassReflectBase {

  /**
    * 动态地创建一个class。通过string代码
    */
  /**
    * 通过字符串创建一个class
    * @param classCode
    * @param className
    * @return
    */
  def createClass(
      classCode: String,
      className: String =
        s"""class_${UUID.randomUUID().toString.replaceAll("-", "")}""") = {
    var clazz = clazzMap.get(classCode)
    if (clazz == null) {
      val zz = compile(prepareScala(className, classCode))
      val methods = zz.getDeclaredMethods // 所有func
      val fields = zz.getDeclaredFields
      clazz = ClassInfo(
        zz,
        zz.newInstance(),
        methods = methods.map { m =>
          (m.getName, m)
        }.toMap,
        fields
      )
      clazzMap.put(classCode, clazz)
    }
    clazz
  }

  /**
    *
    * @param funcCode
    * @param funcName
    * @return
    */
  def createFunc(funcCode: String, funcName: String) = {
    var funcinfo = funcMap.get(funcCode)
    if (funcinfo == null) {
      val className =
        s"""class_${UUID.randomUUID().toString.replaceAll("-", "")}"""
      val classCode =
        s"""class $className{
           | $funcCode
           |}""".stripMargin
      val zz = compile(prepareScala(className, classCode))
      val methods = zz.getDeclaredMethods // 所有func
      funcinfo = FuncInfo(
        funcName,
        zz.newInstance(),
        methods.filter { m =>
          m.getName == funcName
        }.head,
      )
      funcMap.put(funcCode, funcinfo)
    }
    funcinfo
  }
  def main(args: Array[String]): Unit = {
    val intToRow = s"""import org.apache.spark.sql.Row
                      |  def intToRow(itor: Iterator[Row]): Iterator[Row]={
                      |    itor.map(r => Row(r.toSeq.map(_.toString.toUpperCase): _*))
                      |  }""".stripMargin
    val c =
      s"""
         |
         |class HelloClaseeName{
         | var a:String = "hello"
         | ${intToRow}
         |}
         |""".stripMargin

//    val cl = ClassFuncReflectUtils.createClass(c, "HelloClaseeName")
//    cl.methods("intToRow") //.head._2用这个方法不行。因为cl.methods里面有好多方法
//      .invoke(cl.instance, Array(Row("a")).toIterator)
//      .asInstanceOf[Iterator[Row]]
//      .foreach(println)

    val funcinfo = ClassFuncReflectUtils.createFunc(intToRow, "intToRow")
//    funcinfo
//      .methods(funcinfo.funcName)
//      .invoke(funcinfo.instance,Array(Row("a")).toIterator).asInstanceOf[Iterator[Row]]
//      .foreach(println)

    funcinfo.call[Iterator[Row]](Array(Row("a")).toIterator).foreach(println)


  }

}
