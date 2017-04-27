package com.spark.streamsql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.sql.Row
object SparkStreamingSqlTest {
  var sc: SparkContext = null
  var zookeeper: String = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")

  var kafkaParams = Map[String, String](
    "metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
    "serializer.class" -> "kafka.serializer.StringEncoder",
    "group.id" -> "test",
    "zookeeper.connect" -> zookeeper)
  def main(args: Array[String]): Unit = {
    init
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("/user/linmingqiang/checkpoint")
    run(ssc)
  }
  def run(ssc: StreamingContext) {
    val topics = Set("realtimereport_box")
    val sql=new SQLContext(sc)
    import sql.implicits._
    val initialRDD = ssc.sparkContext.parallelize(List(("a", 100,0), ("b", 10,0)))
    
    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    dstream.transform { rdd =>
     val topicAndPartition = KafkaClusterManager.getRDDConsumerOffsets(rdd)
     rdd.map{case(rowkey,d)=>val arr=d.split(",");Reatime(arr(0),arr(1).toInt,arr(2).toInt)}
        .toDF
        .registerTempTable("realtimetmp")
     sql.sql("""select rowkey,sum(bao) as bao,sum(click) as click from realtimetmp group by rowkey""")
        .rdd
        .map { x => (x.getAs[String]("rowkey"),(x.getAs[Long]("bao"),x.getAs[Long]("click"))) }
     //KafkaClusterManager.updateConsumerOffsets(kafkaParams, topicAndPartition)
    }.mapWithState(StateSpec.function(mappingFunc)).print
    ssc.start()
    ssc.awaitTermination()
  }
  def init {
    val sparkConf = new SparkConf()
      .setMaster("local[2]") //3 is cores num,
      .setAppName("UpdateStateByKeyTest")
    sc = new SparkContext(sparkConf)
  }
   val mappingFunc = (table: String, count: Option[(Long, Long)], state: State[(Long, Long)]) => {
    val d = count.getOrElse((0L, 0L))
    val pd = state.getOption.getOrElse((0L, 0L))
    val output = (table, (d._1 + pd._1, d._2 + pd._2))
    state.update((d._1 + pd._1, d._2 + pd._2))
    output
  }
  case class Reatime(rowkey:String,
      bao:Int,
      click:Int)
}