package com.spark.listening

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.core.StreamingKafkaContext
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.ui.StreamingJobProgressListener
import org.apache.spark.streaming.ui.MyStreamingListener
object Test {
  val brokers = "kafka1:9092,kafka2:9092,kafka3:9092"
  def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaWordCountProducer")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingKafkaContext(sc, Seconds(10))
    ssc.ssc.addStreamingListener(new MyStreamingListener(ssc.ssc))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "testGroupid",
      StreamingKafkaContext.WRONG_FROM -> "last", //EARLIEST
      StreamingKafkaContext.CONSUMER_FROM -> "consum")
    val topics = Set("smartadsdeliverylog")
    val dstream = ssc.createDirectStream(kp, topics, msgHandle)
    dstream.foreachRDD { rdd =>
      rdd.take(1).foreach(println)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}