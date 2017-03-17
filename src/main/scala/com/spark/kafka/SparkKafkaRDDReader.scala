package com.spark.kafka

import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkKafkaRDDReader {
  var sc:SparkContext=null
  def main(args: Array[String]): Unit = {
    init
    val topics=Set("realtimereport")
    var kafkaParams = Map[String, String](
       "metadata.broker.list" ->"kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder", 
      "group.id" -> "ZhiZiYunReportStorageRunMain_Box")
   
   val kafkaRdd= KafkaClusterManager.createKafkaRDD(sc, kafkaParams, topics)
    kafkaRdd.take(10).foreach(println)
  }  
   def init() {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    sc = new SparkContext(sparkConf)
  }
}