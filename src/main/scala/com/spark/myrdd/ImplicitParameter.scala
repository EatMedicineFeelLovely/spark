package com.spark.myrdd

trait ImplicitParameter {
  //隐式参数
  implicit val a = Array[String]("@")
  implicit val b = Array[Int](1)
}