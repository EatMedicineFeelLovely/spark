package com.spark.stream.test

import com.spark.learn.test.core.SparkFunSuite
import com.structure.streaming.caseclass.StructureStreamingCaseClass._
import com.structure.streaming.func.StructureStreamingWithStateFunc
import com.structure.streaming.sink.PrintlnSysSink
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
class StructureStreamingKafkaTest extends SparkFunSuite with ParamFunSuite {
  import spark.implicits._
  test("kafka foreachBatch") {
    kafkaDstreaming(kafkabroker)
      .writeStream
//      .option(
//        "checkpointLocation",
//        "/Users/eminem/workspace/learnpro/spark-learn/checkpoint/StructureStreamingKafkaTest$"
//      )
      .foreachBatch { (batchDf: Dataset[Row], batchid: Long) =>
        {
          batchDf.show
        }
      }
      .queryName("foreachBatchSinkQuery") //在listener里面的event.progress.name
      .start()
    spark.streams.awaitAnyTermination(10000)
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
