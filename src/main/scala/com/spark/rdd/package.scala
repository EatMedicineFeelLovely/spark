package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.fun.util.RDDOperateFunction
import com.fun.util.SparkContextOperateFunction
import com.fun.util.SparkContextOperateFunction
import com.fun.util.ZzyLmqDataOperateUtil
package object rdd extends  RDDOperateFunction
                    with SparkContextOperateFunction
                    with ZzyLmqDataOperateUtil{
  //可以通过继承类来获得，也可以直接写
  implicit class SparkContextNewFunction(sparkContext: SparkContext) {
    def lmq(name: String) = ""
  }
  
  //隐式参数的使用
  implicit class RDDNewFunction[T](rdd: RDD[T]) {
    def lmq3(str: String)(implicit impl:Array[T])=rdd.map { x => x + " : "+impl(0) }
    def lmq4[A](str: String)(implicit impl:Array[A])=rdd.map { x => x + " : "+impl(0) }
  }

}