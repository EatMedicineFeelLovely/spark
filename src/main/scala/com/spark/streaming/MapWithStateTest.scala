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

object MapWithStateTest {
  var sc: SparkContext = null
  var zookeeper: String = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    val ssc = new StreamingContext(sc, Seconds(5))
    val initialRDD = ssc.sparkContext.parallelize(List(("a", 100), ("b", 10)))
    ssc.checkpoint("/user/linmingqiang/checkpoint")
    val topics = Set("test")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder", "zookeeper.connect" -> zookeeper)

    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics).map { _._2 }

    val rpt1 = dstream.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_+_)

    rpt1.mapWithState(StateSpec.function(mappingFunc).timeout(Seconds(1))).print
   ssc.start()
    ssc.awaitTermination()

  }

  val mappingFunc = (word: String, count: Option[Int], state: State[Int]) => {
    val sum = count.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("UpdateStateByKeyTest")
    sc = new SparkContext(sparkConf)
  }
}