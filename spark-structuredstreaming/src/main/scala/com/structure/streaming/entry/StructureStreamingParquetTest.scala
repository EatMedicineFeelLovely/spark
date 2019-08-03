package com.structure.streaming.entry

import com.structure.streaming.func.TransFormatFunc
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

object StructureStreamingParquetTest {
  val userSchema = new StructType().add("str", "string")

  /**
    * 读取文件的方式
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

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val csvDF = spark.readStream
      .option("sep", ",") //分隔符
      .schema(userSchema) // Specify schema of the csv files
      .csv("C:\\Users\\Master\\Desktop\\stream_file") //必须是目录
      .as[String]
      .mapPartitions(TransFormatFunc.transCsvToSessionLog)
    //StructureStreamingKafkaTest.mapGroupsWithState(spark,csvDF)
    //StructureStreamingKafkaTest.flatMapGroupsWithState(spark, csvDF)
    streamJoinStream(spark)
    spark.streams.awaitAnyTermination()
  }

  /**
    *
    * @param spark
    */
  def streamJoinStream(spark: SparkSession): Unit ={
    import spark.implicits._
    val clickDF = spark.readStream
      .option("sep", "|") //分隔符
      .schema(userSchema) // Specify schema of the csv files
      .csv("C:\\Users\\Master\\Desktop\\stream_file\\click") //必须是目录
      .as[String]
      .mapPartitions(TransFormatFunc.transToAdClickLog)
      .withWatermark("clickEventTime", "5 minutes") //保留1min的数据

    val impressDF = spark.readStream
      .option("sep", "|") //分隔符
      .schema(userSchema) // Specify schema of the csv files
      .csv("C:\\Users\\Master\\Desktop\\stream_file\\impression") //必须是目录
      .as[String]
      .mapPartitions(TransFormatFunc.transToAdImpressionLog)
      .withWatermark("impressEventTime", "10 minutes") //保留3min的数据


    impressDF
      .join(
        clickDF,
        //interval 5 minutes = watermark( clickEventTime - impressEventTime)
        expr(
          """clickUid = impressUid AND clickEventTime >= impressEventTime AND clickEventTime <= impressEventTime + interval 5 minutes"""
        )
      ) //
      .select("impressdate","clickdate")
      .writeStream
      .outputMode("Append") //只支持append
      .format("console")
      .option("truncate", false)
      .start()



  }
}
