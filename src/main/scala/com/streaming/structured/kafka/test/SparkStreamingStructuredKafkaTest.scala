package com.streaming.structured.kafka.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
object SparkStreamingStructuredKafkaTest {
  System.setProperty("hadoop.home.dir", "C:\\eclipse\\hdplocal2.6.0") //配置spark task之间的数据传输大小

  val kafkabroker = "192.168.0.236:9092,192.168.0.234:9092,192.168.0.235:9092"
  val topics = "testt"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkabroker)
      .option("subscribe", topics)
      .load()
    val wordCounts = df.select("value")
    val query = wordCounts.writeStream
      .outputMode("complete")
      .trigger(ProcessingTime("1 seconds"))//这里就是设置定时器了
      .format("console")
      .start()

    query.awaitTermination()
  }
}