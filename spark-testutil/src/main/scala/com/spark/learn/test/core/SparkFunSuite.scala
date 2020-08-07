package com.spark.learn.test.core

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

abstract class SparkFunSuite extends FunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Test")
    // .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def addStreamListener(): Unit = {

  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()


    //spark.streams.addListener(new StreamingQueryListenerDemo)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    //  spark.stop()
  }

  def kafkaDstreaming(kafkabroker: String) = {
    spark.readStream
      .format("kafka")
      .option("group.id", "test")
      .option("kafka.bootstrap.servers", kafkabroker)
      .option("subscribe", "test,test2") // 可以多个topic，用逗号分开
      .option("startingOffsets", "earliest") //
      .option("failOnDataLoss", "false")
      // .option("auto.offset.reset", "latest")
      .option("maxOffsetsPerTrigger", 115) //每个批次最多拉多少条数据，如果6个分区，这里设置15，那每个分区最多取2条= 12 < 15
      // .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")//指定起点
      .load()
      //.repartition(2)
      .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")
  }
}
