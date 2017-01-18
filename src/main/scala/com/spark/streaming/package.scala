package com.spark

import com.fun.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
package object streaming  extends  RDDOperateFunction
                    with SparkContextOperateFunction
                    with ZzyLmqDataOperateUtil{
  
/*  //隐式参数的使用
  implicit class RDDNewFunction[T](rdd: RDD[T]) {
    def lmq3(str: String)(implicit impl:Array[T])=rdd.map { x => x + " : "+impl(0) }
    def lmq4[A](str: String)(implicit impl:Int)=rdd.map { x => x + " : "}
  }*/

}