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

object UpdateStateByKeyTest {
   var sc: SparkContext = null
    var zookeeper: String = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
    System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
     init
    val ssc=new StreamingContext(sc,Seconds(5))
    val initialRDD = ssc.sparkContext.parallelize(List(("a", 100), ("b", 10)))
    ssc.checkpoint("/user/linmingqiang/checkpoint")
    
    
    val topics = Set("test")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder","zookeeper.connect" -> zookeeper)
    
   val dstream= KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics).map{_._2}
   println(">>>>>>>>>>>>> start "+dstream.count)
   val rpt1=dstream.flatMap(_.split(" ")).map(x => (x, 1))
   //val rpt2=dstream.flatMap(_.split(" ")).map(x => (x+","+x, 1))
  
   val rpt1_dst = rpt1.updateStateByKey[Int](updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), initialRDD)
  rpt1_dst.print()
  /*val rpt2_dst = rpt2.updateStateByKey[Int](updateFunc)
  rpt1_dst.foreachRDD{rdd=>
      rdd.collect().foreach(println)
 }
    rpt2_dst.foreachRDD{rdd=>
      rdd.collect().foreach(println)
    }*/
    
    ssc.start()  
    ssc.awaitTermination()
    
  }

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {  
      val currentCount = values.sum  
      val previousCount = state.getOrElse(0)  
      Some(currentCount + previousCount)  
    }  
   
    def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("UpdateStateByKeyTest")
    sc = new SparkContext(sparkConf)
  }
}