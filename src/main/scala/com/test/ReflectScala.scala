package com.test

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ReflectScala {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setMaster("local").setAppName("ss"))
    
    var a=Class.forName("com.test.ReflectScala")
    //因为该方法是一个静态的方法，所以这个地方的invoke只要填null就可以了。但是如果不是一个静态方法，就需要一个实例
    //a.getMethod("test").invoke(a.newInstance(),"hello world")
    a.getMethod("test",classOf[String]).invoke(null,"hello world")
    
    
    var a2=Class.forName("com.test.ReflectScalaClass")
    a2.newInstance()
      .asInstanceOf[ReflectScalaClass]
      .test2(sc.parallelize(Array("1","2")))
    //a2.getMethod("test2",classOf[RDD[String]]).invoke(a2.newInstance(),sc.parallelize(Array("1","2")))
  }
  def test(s:String){
    println(s)
  }
  
}
class ReflectScalaClass(){
    def test2(rdd:RDD[String]){
    rdd.foreach(println)
  }
  }