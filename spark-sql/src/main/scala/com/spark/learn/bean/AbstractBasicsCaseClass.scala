package com.spark.learn.bean

import scala.reflect.runtime.universe._

/**
  * case class 继承这个抽象类即可
  * @tparam T
  */
abstract class AbstractBasicsCaseClass[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  /**
    * 打印出当前case class 的 各个属性
    * @return
    */
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors
      .map { f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s"  $paramName:\t$paramValue"
      }
      .mkString("{\n", ",\n", "\n}")
  }
}
