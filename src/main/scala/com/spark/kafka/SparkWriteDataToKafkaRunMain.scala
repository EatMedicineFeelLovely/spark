package com.spark.kafka

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
object SparkWriteDataToKafkaRunMain {
  var sc: SparkContext = null
  var ssc: StreamingContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  import org.apache.log4j.{ Level, Logger }
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  val producerConfig = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p.setProperty("zookeeper.connect", "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3")
    p
  }
  val producer = new KafkaProducer[String, String](producerConfig)

  def main(args: Array[String]): Unit = {
    initSCC
    writeDataToKafka
    // send

  }
  def send() {
    for (i <- 1 to 20) {
      val producer = new KafkaProducer[String, String](producerConfig)
      val (rowkey, data) = (1, 2)
      producer.send(new ProducerRecord[String, String]("test", rowkey + "," + data))
      producer.close()
    }

  }

  def writeDataToKafka() {
    //var topics = Set("smartadsdeliverylog")
    var topics = Set("test")
    var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> "test", "zookeeper.connect" -> zookeeper)
    val dstream = KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    dstream.foreachRDD { rdd =>
      println("#############################3333")
      rdd.map {
        case (rowkey, value) =>
          val str = value.split(",")
          (str(0), (rowkey, str))
      }
        .groupByKey
        .foreachPartition { x =>
          var hconf = HBaseConfiguration.create()
          hconf.set("hbase.zookeeper.quorum", zookeeper)
          hconf.set("hbase.zookeeper.property.clientPort", "2181")
          val conn = ConnectionFactory.createConnection(hconf)
          val table = conn.getTable(TableName.valueOf("reportbox_2"))
          for ((tablename, rowjey_str) <- x) {
            for ((rowkey, str) <- rowjey_str) {
              var delivery = str(1).toInt
              var deliveryUV = str(2).toInt
              var cpmcost = str(3).toDouble
              var additionalcpmcost = str(4).toDouble
              var fee = str(5).toDouble
              var fee2 = str(6).toDouble
              var click = str(7).toInt
              var clickUV = str(8).toInt
              var reach = str(9).toInt
              var reachUV = str(10).toInt
              var visitLength = str(11).toInt
              var sencondsClick = str(12).toInt

              val get = new Get(Bytes.toBytes(rowkey))
              val result = table.get(get)
              if (!result.isEmpty()) {
                val hdelivery = result.getValue("info".getBytes, "delivery".getBytes)
                val hdeliveryUV = result.getValue("info".getBytes, "deliveryUV".getBytes)
                val hcpmcost = result.getValue("info".getBytes, "cpmcost".getBytes)
                val hadditionalcpmcost = result.getValue("info".getBytes, "additionalcpmcost".getBytes)
                val hfee = result.getValue("info".getBytes, "fee".getBytes)
                val hfee2 = result.getValue("info".getBytes, "fee2".getBytes)
                val hclick = result.getValue("info".getBytes, "click".getBytes)
                val hclickUV = result.getValue("info".getBytes, "clickUV".getBytes)
                val hreach = result.getValue("info".getBytes, "reach".getBytes)
                val hreachUV = result.getValue("info".getBytes, "reachUV".getBytes)
                val hvisitLength = result.getValue("info".getBytes, "visitLength".getBytes)
                val hsencondsClick = result.getValue("info".getBytes, "sencondsClick".getBytes)

                if (hdelivery!= null) delivery = delivery + new String(hdelivery).toInt
                if (hdeliveryUV!= null) deliveryUV = deliveryUV + new String(hdeliveryUV).toInt
                if (hcpmcost != null) cpmcost = cpmcost + new String(hcpmcost).toDouble
                if (hadditionalcpmcost != null) additionalcpmcost = additionalcpmcost + new String(hadditionalcpmcost).toDouble
                if (hfee != null) fee = fee + new String(hfee).toDouble
                if (hfee2 != null) fee2 = fee2 + new String(hfee2).toDouble
                if (hclick != null) click = click + new String(hclick).toInt
                if (hclickUV != null) clickUV = clickUV + new String(hclickUV).toInt
                if (hreach != null) reach = reach + new String(hreach).toInt
                if (hreachUV != null) reachUV = reachUV + new String(hreachUV).toInt
                if (hvisitLength != null) visitLength = visitLength + new String(hvisitLength).toInt
                if (hsencondsClick != null) sencondsClick = sencondsClick + new String(hsencondsClick).toInt
                
                
                
              }
              val put = new Put(Bytes.toBytes(rowkey))
              put.addColumn("info".getBytes, "delivery".getBytes, delivery.toString().getBytes)
              put.addColumn("info".getBytes, "deliveryUV".getBytes, deliveryUV.toString().getBytes)
              put.addColumn("info".getBytes, "cpmcost".getBytes, cpmcost.toString().getBytes)
              put.addColumn("info".getBytes, "additionalcpmcost".getBytes, additionalcpmcost.toString().getBytes)
              put.addColumn("info".getBytes, "fee".getBytes, fee.toString().getBytes)
              put.addColumn("info".getBytes, "fee2".getBytes, fee2.toString().getBytes)
              put.addColumn("info".getBytes, "click".getBytes, click.toString().getBytes)
              put.addColumn("info".getBytes, "clickUV".getBytes, clickUV.toString().getBytes)
              put.addColumn("info".getBytes, "reach".getBytes, reach.toString().getBytes)
              put.addColumn("info".getBytes, "reachUV".getBytes, reachUV.toString().getBytes)
              put.addColumn("info".getBytes, "visitLength".getBytes, visitLength.toString().getBytes)
              put.addColumn("info".getBytes, "sencondsClick".getBytes, sencondsClick.toString().getBytes)
              table.put(put)

            }
          }
          table.close()
          conn.close()
        }

      //rdd.writeToKafka(producerConfig,s=>new ProducerRecord[String, String]("test", "@@@@@@"))
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@22")
    }
    ssc.start()
    ssc.awaitTermination()
  }
  def initSC() {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)

  }
  def initSCC() {
    if (sc == null) {
      initSC
    }
    ssc = new StreamingContext(sc, Seconds(30))
  }

  def tran(s: (String, String)) = new ProducerRecord[String, String]("test", s._1)

}