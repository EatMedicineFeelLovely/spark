

package com.fun.util

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag
import org.apache.spark.streaming.mysql.DirectMysqlInputDStream
import org.apache.spark.streaming.mysql.JdbcSparkStreamRDD

trait RDDOperateFunction {
  //第一种方法
  implicit def rddFunction[T](rdd:RDD[T])=new RDDFunctionToClassTag(rdd)
  class RDDFunctionToClassTag[T](rdd:RDD[T]){
    //在这里面定义方法
    def printlnRDD()=rdd.foreach { println }
  }
  //第二种方法
  implicit class RDDFunctionToString(rdd:RDD[String]){
     def rddF2(str:String)=rdd.map { x => x+" : "+str }
     def rddF(str:String)=rdd.map { x => x+" : "+str }
  }
  implicit class DStreamFunc[A<: InputDStream[(String,String)]](dstream:A){
    def printlnDStream(str:String)=dstream.foreachRDD(rdd=>rdd.collect.foreach(x=>println(str+x)))
  }
  implicit def printlnDStream2(rdd:JdbcSparkStreamRDD[(String, String)])=rdd.collect.foreach(println)
}