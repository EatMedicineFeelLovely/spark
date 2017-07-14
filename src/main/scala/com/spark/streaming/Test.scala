package com.spark.streaming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.mysql.MysqlManager
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD
object Test {
  var sc: SparkContext = null
  import org.apache.log4j.{ Level, Logger }
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  def init() {
    val sparkConf = new SparkConf()
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
  def main(args: Array[String]): Unit = {
    val plan=args(1)
    val siteid=args(0)
    var count=0L
    init
    val ssc = new StreamingContext(sc, Seconds(15))
    var topics = Set("smartadsdeliverylog")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder", 
      "group.id" -> "test", 
      "zzy.kafka.todayoffset"->"smartadsdeliverylog,1,1546817452|smartadsdeliverylog,9,5950067|smartadsdeliverylog,4,1507038159|smartadsdeliverylog,5,5950065|smartadsdeliverylog,6,5950069|smartadsclicklog,0,63041569|smartadsdeliverylog,0,2586769008|smartadsdeliverylog,7,5950064|smartadsdeliverylog,2,1543841065|smartadsdeliverylog,8,5950075|smartadsdeliverylog,3,1506592910",
      "zookeeper.connect" -> zookeeper)
    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    dstream.foreachRDD{rdd=>
      val a=rdd.take(1)(0)._2
      println(a)
      
      count=count+rdd.filter{x=>
         val datas=x._2.split(",")
         datas(0).substring(0, 10)=="2017-05-10" && datas(14)==siteid && datas(25)==plan
      }.count
      println(">>>>　：　"+count)
      if(a.startsWith("2017-05-11")){
        System.exit(0)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}