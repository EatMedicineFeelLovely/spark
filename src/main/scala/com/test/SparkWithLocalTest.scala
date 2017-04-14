package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar
import java.util.ArrayList
import org.apache.hadoop.fs.Path
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.Broker
import kafka.common.TopicAndPartition
object SparkWithLocalTest {
 var sc: SparkContext = null
 val zookeeper=""
 System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
 val correctData=new ArrayList[(String,String,String,Int)] 
 def main(args: Array[String]): Unit = {
   val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf) 
   
   
   
 }
 
 def getKafkaRDD(){
   var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test", "zookeeper.connect" -> zookeeper)
   KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder, (String, String)](
       sc, 
       kafkaParams,
       null, 
       Map[TopicAndPartition, Broker](), 
       (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
   
 
 
 }
 
 
 
 def peixu(){
  val rdd= sc.textFile("/data/test")
  rdd.flatMap{x=>x.split(" ")}
     .map{x=>(x,1)}
     .reduceByKey(_+_)
     .sortBy({case(key,num)=>num},false)
     .foreach(println)
 }
 def runJob(){
   var rdd=sc.parallelize(Array((0,0)))
     var tmprdd=sc.parallelize(Array((0,1)))
                  .map{x=>println("@");x}
     val rrdd=tmprdd.groupByKey//在有shuffle操作的时候，spark默认会对其数据进行缓存，不会重新计算
                    .map{x=>println("##");(x._1,x._2)}
     rrdd.foreach(println)
     rrdd.foreach(println)
         
   

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