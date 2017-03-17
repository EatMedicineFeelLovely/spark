package com.spark.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import com.mysql.jdbc.Connection
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.transport.LocalTransportAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.net.InetAddress
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.google.gson.GsonBuilder
import net.sf.json.JSONObject
import org.elasticsearch.common.xcontent.XContentFactory
import scala.collection.JavaConverters._
object Test {
  var sc: SparkContext = null
  def main(args: Array[String]): Unit = {
    /*val client = getESClient
    println(client.listedNodes())
    val bulk = client.prepareBulk()
    */
     val client=getESClient
    queryES(client)
    /*val builder = XContentFactory.jsonBuilder()
      .startObject()
      .field("firstName", "Avivi")
      .field("map", Map("age"->1,"age2"->2).asJava)
      .endObject()
      
    val request = client.prepareIndex("test", "testType")
                        .setSource(builder)
    bulk.add(request)
    val response = bulk.get
    response.getItems.foreach { x => println(!x.isFailed()) }*/
  }
def queryES(client: TransportClient){
 val d= client.prepareGet("sdr_urlinfo_test","urlinfo","http%3A%2F%2Fbaojian.zx58.cn%2Fproduct%2F9348%2F")
 .setFetchSource("frequency", "").get
 println(d.getField("frequency"))
}
  def getESClient() = {
    val endpoints = Array("192.168.10.115", "192.168.10.110", "192.168.10.81")
      .map(_.split(':')).map {
        case Array(host, port) => SocketEndpoint(host, port.toInt)
        case Array(host)       => SocketEndpoint(host, 9300)
      }
    val settings = Map("cluster.name" -> "zhiziyun")
    val esSettings = Settings.settingsBuilder().put(settings.asJava).build()
    val client = TransportClient.builder().settings(esSettings).build()
    val addresses = endpoints.map(endpointToTransportAddress)
    client.addTransportAddresses(addresses: _*)
    client
  }

  def endpointToTransportAddress(endpoint: Endpoint): TransportAddress = endpoint match {
    case LocalEndpoint(id)             => new LocalTransportAddress(id)
    case SocketEndpoint(address, port) => new InetSocketTransportAddress(InetAddress.getByName(address), port)
  }

  def init {
    val sparkConf = new SparkConf()
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
}
case class SocketEndpoint(address: String, port: Int) extends Endpoint
case class LocalEndpoint(id: String) extends Endpoint
sealed abstract class Endpoint