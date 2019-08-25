package com.spark.stream.test

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.structure.streaming.entry.{kafkabroker, topics}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

object Test {
  System.setProperty("hadoop.home.dir", "D:\\hadoop_home_2.7.1")
  val  topics = "pv"
  val kafkabroker = "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:909"
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("tt")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("group.id", "test")
      .option("kafka.bootstrap.servers", kafkabroker)
      .option("subscribe", topics) // 可以多个topic，用逗号分开
      .option("startingOffsets", "earliest") //
      .option("maxOffsetsPerTrigger", 1) //每个批次最多拉多少条数据，如果6个分区，这里设置15，那每个分区最多取2条= 12 < 15
      // .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")//指定起点
      .load()
      //.repartition(2)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    foreachBatchSink(spark, df)

    spark.streams.awaitAnyTermination()

  }

  /**
   * 对每个batch数据做处理
   * 10s种一个批次
   *
   * @param df
   */
  def foreachBatchSink(spark: SparkSession, df: Dataset[String]): Unit = {
    //对每个批次做处理，这里跟sparkstreaming很像了，只是去掉batch time的概念，
    val func = (batchDf: Dataset[String], batchid: Long) => {
      println(batchid, batchDf.count())
      batchDf.take(1).foreach(println)
    }
//    spark.sparkContext
//         .setLocalProperty("spark.scheduler.pool", "pool_foreachBatchSink")
    df.writeStream
      //.trigger(Trigger.ProcessingTime("1 seconds"))

      .foreachBatch(func)
      .queryName("foreachBatchSinkQuery") //在listener里面的event.progress.name
      .start()
  }
}
