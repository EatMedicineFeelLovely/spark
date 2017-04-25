package com.spark.test
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession

object TestKafka080 {
  var kafkaParams = Map[String, String](
    "metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
    "serializer.class" -> "org.apache.kafka.common.serialization.StringSerializer",
    "group.id" -> "test")
  val topics = Set("realtimereport_box")

  def main(args: Array[String]): Unit = {
    run()
   val spark = SparkSession.builder().getOrCreate()
   
    import spark.implicits._
    //kafak
   val a= spark.readStream.format("kafka")
    .load()
   
    spark.read.textFile("examples/src/main/resources/people.json")
  }
  def run() {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
    val ssc = new StreamingContext(sc, Seconds(5))
    val r = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    r.foreachRDD { rdd => println("<<>>>>>>"); rdd.foreach(println) }

    ssc.start()
    ssc.awaitTermination()
  }

}