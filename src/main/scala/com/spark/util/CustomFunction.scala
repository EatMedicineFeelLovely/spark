package com.spark.util

import org.apache.spark.rdd.RDD
/*
 * 这里自定义的方法，可以作用于所有的rdd
 * 
 */
object CustomFunction {
  //implicit是隐式的的意思。import CustomFunctions._之后，所有的rdd就可以使用下面的函数
  //这个使用方式和是         addIteblogCustomFunctions(rdd)而不能rdd.addIteblogCustomFunctions
  //而下面那个是                  rdd.totalSales()  rdd.totalSales2()这样用
  implicit def addIteblogCustomFunctions(rdd: RDD[(String,Double)])= rdd.map(_._2).sum 
  //可以使用CustomFunctions2下的所有函数。而且是rdd.totalSales()这样用
  implicit def addIteblogCustomFunctions2(rdd: RDD[(String,Double)])=new CustomFunctions2(rdd)
  implicit def addIteblogCustomFunctions(rdd: RDD[String])=new CustomFunctions3(rdd)
  implicit def test2(rdd: RDD[String],data:String)=new MySelfRDD(rdd,data)
}
class CustomFunctions2(rdd:RDD[(String,Double)]) {
  def totalSales() = rdd.map(_._2).sum 
  def totalSales2() = rdd.map(_._2).sum
}

class CustomFunctions3(rdd:RDD[String]) {
  //将两个字符串合并
  def  mergeString(data:String) = new MySelfRDD(rdd,data)
  def test(rdd: RDD[String])=null
}