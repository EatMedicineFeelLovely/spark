package com.spark.scala

trait ImplicitClass {
  implicit def toD(str:String)=str.toDouble
}