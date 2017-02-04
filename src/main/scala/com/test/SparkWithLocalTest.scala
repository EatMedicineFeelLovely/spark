package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar
import java.util.ArrayList
object SparkWithLocalTest {
 var sc: SparkContext = null
 System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
 val correctData=new ArrayList[(String,String,String,Int)] 
 def main(args: Array[String]): Unit = {
   //init 
   //runJob
    println(List(1,2,3,4,5,6).reduce((a,b)=>a+b))
 }
 def runJob(){
   val rdd=sc.parallelize(Array(1,2,3,4,5,6,7,8))
   val max= rdd.reduce((a,b)=>Math.max(a, b))
   println(max) 
 }
 
  def sparkTest(){
   val rdd=sc.parallelize(Array((1,6),(7,8),(9,1)),3).zipWithIndex().map(x=>(x._2,x._1))
   rdd.foreach(println)
   val rdd2=rdd.map{x=>
     var index=x._1-1
       (index,x._2)
   }
   rdd2.foreach(println)
   rdd.join(rdd2).map{x=>
     val (f,s)=x._2
     (s._1,s._2-f._2)}.foreach(println)
  }
     
    def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
}