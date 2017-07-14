package org.apache.spark.streaming.kafka

import java.io.Serializable
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import org.apache.commons.logging.LogFactory
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext

object KafkaClusterManager {
  var topics: Set[String] = null
  var kafkaParams: Map[String, String] = null
  var kc: KafkaCluster = null
  var groupId: String = "Test"
  def getKafkafromOffsets(topics: Set[String], kafkaParams: Map[String, String]) = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    //有两个参数"largest"/"smallest"，一个是从最新，一个是从最头开始读数据
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      val fromOffsets = leaderOffsets.map {
        case (tp, lo) =>
          (tp, lo.offset)
      }
      fromOffsets
    }).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
    fromOffsets
  }
  def getConsumerOffsetsByToday() = {
    var consumerOffsets = new HashMap[TopicAndPartition, Long]()
    var todayOffsets = kafkaParams.get("zzy.kafka.todayoffset").get.split('|')
    for (offset <- todayOffsets) {
      val offsets = offset.split(",")
      consumerOffsets.put(new TopicAndPartition(offsets(0), offsets(1).toInt), offsets(2).toLong)
    }
    consumerOffsets.toMap
  }
  def createDirectStream(ssc: StreamingContext,
                         kafkaParams: Map[String, String],
                         topics: Set[String]) = { //先获取这个groupid所消费的offset
    this.kafkaParams = kafkaParams
    this.topics = topics
    groupId=kafkaParams("group.id")
    kc = new KafkaCluster(kafkaParams)
    var consumerOffsets: Map[TopicAndPartition, Long] = getConsumerOffsets(topics, groupId)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaParams,
      consumerOffsets,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))

  }
  /**
   * 用于sc创建kafkaRDD
   */
  def createKafkaRDD(
    sc: SparkContext,
    kafkaParams: Map[String, String],
    topics: Set[String]) = {
    this.kafkaParams = kafkaParams
    this.topics = topics
    kc = new KafkaCluster(kafkaParams)
    var fromOffsets: Map[TopicAndPartition, Long] = getConsumerOffsets(topics, kafkaParams.get("group.id").getOrElse("realtimereport"))
    println(">>>>>>>>>>>>>>>from ")
    fromOffsets.foreach(println)

    val maxMessagesPerPartition = sc.getConf.getInt("spark.streaming.kafka.maxRatePerPartition", 0) //0表示没限制
    val lastestOffsets = latestLeaderOffsets(fromOffsets)
    val untilOffsets = if (maxMessagesPerPartition > 0) {
      latestLeaderOffsets(fromOffsets).map {
        case (tp, lo) =>
          tp -> lo.copy(offset = Math.min(fromOffsets(tp) + maxMessagesPerPartition, lo.offset))
      }
    } else lastestOffsets
    val leaders = untilOffsets.map { case (tp, lo) => tp -> Broker(lo.host, lo.port) }.toMap
    val offsetRanges = fromOffsets.map {
      case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray
    println(">>>>>>>>>>>>>>>offsetRanges ")
    offsetRanges.foreach(println)
    
    KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder, (String, String)](
      sc,
      kafkaParams,
      offsetRanges,
      leaders,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
  }
  protected final def latestLeaderOffsets(consumerOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(consumerOffsets.keySet)
    if (o.isLeft) {
      throw new SparkException(o.left.toString)
    } else {
      o.right.get
    }
  }

  /**
   * 创建数据流前，根据实际消费情况更新消费offsets
   * @param topics
   * @param groupId
   */
  private def getConsumerOffsets(topics: Set[String], groupId: String) = {
    var offsets: Map[TopicAndPartition, Long] = Map()
    topics.foreach(topic => {
      var hasConsumed = true //是否消费过  ,true为消费过
      val partitionsE = kc.getPartitions(Set(topic)) //获取patition信息
      if (partitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息      
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为latestLeaderOffsets          
        consumerOffsets.foreach({
          case (tp, n) =>
            //现在数据在什么offset上
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              //消费过，但是过时了，就从头消费(或者从最新开始消费)
              val latestLeaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get(tp).offset
              offsets += (tp -> latestLeaderOffsets)
            } else offsets += (tp -> n) //消费者的offsets正常
        })
      } else { // 没有消费过 ，这是一个新的消费group id
        println(">>>>>>>>>>>>>>>>>>>>>这是一个新消费者："+groupId)
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        }
        //如果这里不对这个新消费者进行updateConsumerOffsets的话，那下次再来还是个新消费者
        leaderOffsets.foreach { case (tp, offset) => offsets += (tp -> offset.offset) }
      }
    })
    offsets

  }
  def getRDDConsumerOffsets[T](data: RDD[T]) = {
    var consumoffsets = Map[TopicAndPartition, Long]()
    val offsetsList = data.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      consumoffsets += ((topicAndPartition, offsets.untilOffset))
    }
    consumoffsets
  }
  /**
   * 更新zookeeper上的消费offsets
   * @param rdd
   */
  def updateConsumerOffsets(kp: Map[String, String],topicAndPartition: Map[TopicAndPartition, Long]): Unit = {
    if(kc==null){
      kc=new KafkaCluster(kp)
    }
    val o = kc.setConsumerOffsets(kp.get("group.id").get, topicAndPartition)
  }
}