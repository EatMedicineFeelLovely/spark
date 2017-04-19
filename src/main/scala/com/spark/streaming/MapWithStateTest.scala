package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.State
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.SocketInputDStream
import org.apache.spark.streaming.kafka.KafkaClusterManager._
object MapWithStateTest {
  var sc: SparkContext = null
  var zookeeper: String = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    val ssc = new StreamingContext(sc, Seconds(5))
    // val initialRDD = ssc.sparkContext.parallelize(List(("a", 100), ("b", 10)))
    ssc.checkpoint("/user/linmingqiang/checkpoint")
    val topics = Set("test")
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "zookeeper.connect" -> zookeeper)

    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    dstream.foreachRDD{data=>
      println(">>>>>> : "+data.count)
    val topicAndPartition=getRDDConsumerOffsets(data)
    //updateConsumerOffsets(kafkaParams, topicAndPartition)
    }
    val rpt1 = dstream.map {
      case (key, x) =>
        val arr = x.split(",");
        ((arr(0), key), (arr(1).toInt, arr(2).toInt))
    }
      .reduceByKey { (x, y) => (x._1 + y._1, x._2 + y._2) }

    rpt1.mapWithState(StateSpec.function(mappingFunc)).print
    
    ssc.start()
    ssc.awaitTermination()

  }

  val mappingFunc = (word: (String, String), count: Option[(Int, Int)], state: State[(Int, Int)]) => {
    val d = count.getOrElse((0, 0))
    val pd = state.getOption.getOrElse((0, 0))
    val output = (word, (d._1 + pd._1, d._2 + pd._2))
    state.update((d._1 + pd._1, d._2 + pd._2))
    output
  }

  def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("UpdateStateByKeyTest")
    sc = new SparkContext(sparkConf)
  }
}