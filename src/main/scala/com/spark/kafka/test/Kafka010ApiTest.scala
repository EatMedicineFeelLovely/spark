package com.spark.kafka.test

import org.apache.spark.streaming.kafka010.KafkaUtils
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
object Kafka010ApiTest {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "true"); // 自动commit
    props.put("auto.commit.interval.ms", "1000"); // 自动commit的间隔
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List("test1", "test2")); // 可消费多个topic,组成一个list
    val records = consumer.poll(100)
   records.foreach(x=>x.timestamp())
  }
}