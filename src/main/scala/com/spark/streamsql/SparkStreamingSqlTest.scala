package com.spark.streamsql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.sql.SQLContext
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
    val topics = Set("test")
    val sql=new SQLContext(sc)
    import sql.implicits._
    val data=sc.parallelize(Array[String]()).map{_=>Reatime(null,0,0)}
    sql.createDataFrame(data)
              .toDF
              .registerTempTable("realtime")
    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    
    dstream.foreachRDD { rdd =>
      val topicAndPartition = KafkaClusterManager.getRDDConsumerOffsets(rdd)
     rdd.map{case(rowkey,d)=>
      val arr=d.split(",")
      Reatime(arr(0),arr(1).toInt,arr(2).toInt)}
     .toDF
     .registerTempTable("realtimetmp")
     
     sql.sql("""
       select IF(a.rowkey is NULL,b.rowkey,a.rowkey) as rowkey,
         IF(a.bao is NULL,b.bao,IF(b.bao is NULL,a.bao,a.bao+b.bao)) as bao,
         IF(a.click is NULL,b.click,IF(b.click is NULL,a.click,a.click+b.click)) as click 
        from realtime a full join 
       (select rowkey,sum(bao) as bao,sum(click) as click 
       from realtimetmp group by rowkey) b
       on a.rowkey=b.rowkey
       """).registerTempTable("realtime")
       sql.cacheTable("realtime")
       sql.sql("select * from realtime").show
     KafkaClusterManager.updateConsumerOffsets(kafkaParams, topicAndPartition)
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
  def init {
    val sparkConf = new SparkConf()
      .setMaster("local[3]") //3 is cores num,
      .setAppName("UpdateStateByKeyTest")
    sc = new SparkContext(sparkConf)
  }
  case class Reatime(rowkey:String,
      bao:Int,
      click:Int)
}