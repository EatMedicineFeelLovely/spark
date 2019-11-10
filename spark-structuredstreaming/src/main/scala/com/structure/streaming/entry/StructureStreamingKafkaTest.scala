package com.structure.streaming.entry

import com.structure.streaming.sink.PrintlnSysSink
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import com.structure.streaming.caseclass.StructureStreamingCaseClass._
import com.structure.streaming.func.{
  StructureStreamingWithStateFunc,
  TransFormatFunc
}
import com.structure.streaming.listener.StreamingQueryListenerDemo
import org.apache.ivy.plugins.trigger.Trigger
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.HasOffsetRanges
object StructureStreamingKafkaTest {
  //PropertyConfigurator.configure("log4j.properties")
// 由于Flink与Structured Streaming的架构的不同，task是常驻运行的，flink不需要状态算子
  // 同样的分区id 会发往同样的task。不像spakr每次都释放，然后再重新申请task，每次分区都会重新在不同的executor上执行
  // 这导致了spark不能用rocksdb
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
      .option("maxOffsetsPerTrigger", 5) //每个批次最多拉多少条数据，如果6个分区，这里设置15，那每个分区最多取2条= 12 < 15
      // .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")//指定起点
      .load()
      //.repartition(2)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    //mapGroupsWithState(spark, df.mapPartitions(TransFormatFunc.transToSessionLog))
    // aggregate(spark, df.mapPartitions(TransFormatFunc.transToAdlog))
    //waterMarkWindow(spark, df.mapPartitions(TransFormatFunc.transToAdlog))
    //foreachSink(df.mapPartitions(TransFormatFunc.transToAdlog))
    foreachBatchSink(spark, df)
    spark.streams.addListener(new StreamingQueryListenerDemo)
    // spark.streams.awaitAnyTermination()

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
    spark.sparkContext
      .setLocalProperty("spark.scheduler.pool", "pool_foreachBatchSink")
    df.writeStream
    // .trigger(Trigger.ProcessingTime("1 seconds"))
//      .option(
//        "checkpointLocation",
//        "C:\\Users\\Master\\Desktop\\checkpointDir2"
//      )
      .foreachBatch(func)
      .queryName("foreachBatchSinkQuery") //在listener里面的event.progress.name
      .start()
    // .awaitTermination()
  }

  /**
    *   foreach data sink
    *
    * @param df
    */
  def foreachSink(df: Dataset[AdlogData]): Unit = {
    //foreach sink，如果是落地hbase，还是用batch来吧，其实所有的流式最终落地都是走的批次，否则一条写一次代价太大
    df.writeStream
      .outputMode("update")
      .foreach(new PrintlnSysSink())
      .queryName("foreachSink")
      .start()

  }

  /**
    * 每20s 计算一次1分钟前的窗口数据，延迟1分钟
    * 注意：只设置 withWatermark 是达不到顾虑数据的作用的，例如设置了10min，数据延迟了site还是会统计。
    *
    * @param df
    */
  def waterMarkWindow(spark: SparkSession, df: Dataset[AdlogData]) = {
    import spark.implicits._
    //每个窗口内的数据以word来分组，并对word做count(*)统计= select word,count(1) from window group by word
    //watermark的作用其实就是来决定 某个windows 的key要不要被清除，
    //watermark = max(eventime) - ${delay min} ,如果window[max time] < watermark ,那这个窗口就扔了，不会再更新了
    df.withWatermark("time", "10 minutes")
      //这里的window也是个字段，数据为 ：[2019-07-19 15:03:00.0,2019-07-19 15:04:00.0]
      .groupBy(window($"time", "1 minutes", "1 minutes"), $"site")
      .agg(count("*") as "siteCount")
      .writeStream
      .outputMode("update") //
      .format("console")
      .option("truncate", false)
      .start()

  }

  /**
    * 计算session.统计每个sessionid的开始访问日期和结束日期，和访问次数
    * withWatermark ： 如果某个sessionid 超过一定时间不更新那就丢了 。 watermark类似filter
    *
    * @param spark
    * @param df
    */
  def aggregate(spark: SparkSession, df: Dataset[AdlogData]) = {
    import spark.implicits._
    spark.sparkContext
      .setLocalProperty("spark.scheduler.pool", "pool_aggregate")
    val outstream = df //.withWatermark("eventTime", "1 minutes")
      .groupBy($"site") //
      .agg(
        min("timestamp") as "minTime",
        max("timestamp") as "maxTime",
        count("*").cast("string").as("count")
      ) //cast修改字段类型
      .where($"count" > 1)

    //outstream.printSchema()
    val query = outstream.writeStream
    //可以通过修改checkpoint->offsets 下的batchid 文件里面的offset来决定下次从哪里offset读取数据
    //.option("checkpointLocation", "C:\\Users\\Master\\Desktop\\checkpointDir")
      .outputMode("update") //
      .format("console")
      .option("truncate", false)
      .queryName("aggregateQuery")
      .start()
  }

  /**
    * 由用户自己管理状态.计算session.。计算各个sessionid的步长
    *
    * @param spark
    * @param df
    */
  def mapGroupsWithState(spark: SparkSession, df: Dataset[SessionLog]) = {
    import spark.implicits._
    //当使用了GroupStateTimeout.EventTimeTimeout ，就必须使用watermark
    //如果event time max = 2019-01-01 00:00:03 -> watermark =  2019-01-01 00:00:01 。event time < watermark直接扔了
    df.withWatermark("time", "10 seconds")
      .groupByKey(_.sessionid)
      .mapGroupsWithState[SessionStateInfo, SessionResult](
        GroupStateTimeout.EventTimeTimeout
      )( //GroupStateTimeout.EventTimeTimeout)(
        StructureStreamingWithStateFunc.sessionAggMapWithState)
      .writeStream
      //失败后直接重启即可，但是checkpoint恢复有一些限制，代码基本不能修改
      //.option("checkpointLocation", "C:\\Users\\Master\\Desktop\\checkpointDir")
      .format("console")
      .option("truncate", false)
      .outputMode("update") //只支持update
      .start()

  }

  /**
    * 由用户自己管理状态.计算session.。计算各个sessionid的步长
    *
    * @param spark
    * @param df
    */
  def flatMapGroupsWithState(spark: SparkSession, df: Dataset[SessionLog]) = {
    import spark.implicits._
    //当使用了GroupStateTimeout.EventTimeTimeout ，就必须使用watermark
    //event time max = 2019-01-01 00:00:03 -> watermark =  2019-01-01 00:00:01 。
    //event time < watermark直接扔了
    val func = (batchDf: Dataset[SessionResult], batchid: Long) => batchDf.show
    df.withWatermark("time", "10 seconds")
      .groupByKey(_.sessionid)
      .flatMapGroupsWithState[SessionStateInfo, SessionResult](
        OutputMode.Update(),
        GroupStateTimeout.EventTimeTimeout
      )( //GroupStateTimeout.EventTimeTimeout)(
        StructureStreamingWithStateFunc.sessionAggFlatMapWithState)
      .writeStream
      //失败后直接重启即可，但是checkpoint有一些限制，代码基本不能修改
      //.option("checkpointLocation", "C:\\Users\\Master\\Desktop\\checkpointDir")
      //.format("console")
      .option("truncate", false)
      .outputMode("update") //只支持update
      .foreachBatch(func)
      .start()

  }

}
