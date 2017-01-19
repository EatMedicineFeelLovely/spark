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
   init 
   
   for(i<- 1 to 10){
     //模拟数据实时数据
     Thread.sleep(1000)//1秒钟一次
     runJob
   }
   
 }
 def runJob(){
   val rdd=sc.parallelize(Array(("1","1","1",3),("2","2","2",3),("3","3","3",3),("4","4","4",3),("5","5","5",3),("6","6","6",3),("7","7","7",3)))
   val d=rdd.mapPartitions{x=>
     //把数据如数据库操作
     x}
     .collect()
   //写一个方法和数据库里面的进行比对
   bidui(d)
   
   
 }
 def bidui(d:Array[(String, String, String, Int)]){
   for((kpi_id,priv_id,date,kpi)<-d){
     //按照数据去数据库取数据，然后比对
     val mysql_data=1//从数据库取出的数据
     if(Math.abs(mysql_data-kpi)>1){//差值大于1
       correctData.add((kpi_id,priv_id,date,kpi))//把符合条件的加进来
       if(correctData.length==6){
         //已经连续6条满足条件了
         //你自己看做什么操作
         println(correctData)
         correctData.clear()//清空
       }
     }else{
       correctData.clear()//不满足就清空
     }
   }
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