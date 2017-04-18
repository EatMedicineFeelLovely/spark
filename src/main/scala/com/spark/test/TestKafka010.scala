package com.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils

/*import org.apache.spark.streaming.kafka010.KafkaUtils*/
/*import org.apache.spark.streaming.kafka010._*/
/*import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe*/
object TestKafka010 {
  val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "192.168.10.84:9092,192.168.10.85:9092,192.168.10.86:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "test",
  //"auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)
val topics = Array("realtimereport_box")
 def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
    
    
  }

/*def kafka010(){
     val sc=new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
    val ssc=new StreamingContext(sc,Seconds(5))
    val r=KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,//用于spark的executor读取kafka数据分区的策略，consistent和fix（自己手动指定书目分区读到哪个host）
        Subscribe[String, String](topics, kafkaParams))//自己定义offset的起点还是由默认的
      r.foreachRDD{rdd=>println("<<>>>>>>");rdd.foreach(println)}  
    
    ssc.start()
    ssc.awaitTermination()
       
  }*/
}