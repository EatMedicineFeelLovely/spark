package com.structure.streaming.entry

import com.structure.streaming.caseclass.StructureStreamingCaseClass._
import com.structure.streaming.func.TransFormatFunc
import com.structure.streaming.listener.StreamingQueryListenerDemo
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object StructureStreamingStreamJoinTest {
  System.setProperty("user.timezone", "GMT+8")

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("tt")
        .master("local[*]")
        .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC") //设置时区，否则少8小时
    spark.sparkContext.setLogLevel("ERROR")
    val impressionDf = getImpressionDf(spark)
    val clickDf = getClickDf(spark)
    //如果点击比曝光晚5min那就丢弃了 clickEventTime <= impressEventTime + interval 5 minutes
    //例如01的曝光，06的点击，这已经超过5分钟了，所以不能join 。所以点击Watermark 晚5分钟
    //只允许以当前时间为基准向前推5min内的触达数据和10min内的点击数据进行join，超出这个时间界限的数据不会被Stream State维护，避免无止尽的State。
    //printlnDf(spark, impressionDf)
    streamJoinStream(impressionDf, clickDf)
    spark.streams.addListener(new StreamingQueryListenerDemo())
    spark.streams.awaitAnyTermination()

  }

  /**
    *
    * @param impressionDf
    */
  def printlnDf(spark: SparkSession,
                impressionDf: Dataset[AdImpressionLog]): Unit = {
    import spark.implicits._
    impressionDf
      .groupBy($"impressEventTime", $"impressUid") //这样watermark能起到作用，因为state有impressEventTime可以来判断清理
//      .groupBy($"impressUid"),//这样不行，因为state里面没有eventtime
      .agg(count("*") as "count")
      .writeStream
      .outputMode("update") //只支持append
      .format("console")
      .option("truncate", false)
      .queryName("StreamJoinQuery")
      .start()
  }

  /**
    *
    * @param spark
    * @return
    */
  def getClickDf(spark: SparkSession) = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("group.id", "test")
      .option("kafka.bootstrap.servers", kafkabroker)
      .option("subscribe", testClickTopic) // 可以多个topic，用逗号分开
      .option("startingOffsets", "latest") //
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .mapPartitions(TransFormatFunc.transToAdClickLog)
      .withWatermark("clickEventTime", "10 minutes") //保留1min的数据

  }

  /**
    *
    * @param spark
    * @return
    */
  def getImpressionDf(spark: SparkSession) = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("group.id", "test")
      .option("kafka.bootstrap.servers", kafkabroker)
      .option("subscribe", testImpressTopic) // 可以多个topic，用逗号分开
      .option("startingOffsets", "latest") //
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .mapPartitions(TransFormatFunc.transToAdImpressionLog)
      .withWatermark("impressEventTime", "3 minutes") //保留3min的数据
  }

  /**
    * 两个流join ，watermark是选用值更小的那个stream watermark作为最终的watermark。所有click延迟更久一点以获得更早的watermark
    * 因为在当前的时间点上，click要比impression数据来得晚
    * @param impressionDf
    * @param clickDf
    * @return
    */
  def streamJoinStream(impressionDf: Dataset[AdImpressionLog],
                       clickDf: Dataset[AdClickLog]) = {
    impressionDf
      .join(
        clickDf,
        //interval 5 minutes = watermark( clickEventTime - impressEventTime)
        expr(
          """clickUid = impressUid AND clickEventTime >= impressEventTime AND clickEventTime <= impressEventTime + interval 5 minutes"""
        )
      ) //
      .select("impressdate", "clickdate")
      .writeStream
      .outputMode("Append") //只支持append
      .format("console")
      .option("truncate", false)
      .queryName("StreamJoinQuery")
      .start()
  }
}
