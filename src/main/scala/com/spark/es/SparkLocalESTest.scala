package com.spark.es

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.elasticsearch.common.xcontent.XContentFactory
import scala.collection.JavaConverters._
object SparkLocalESTest {
   var sc: SparkContext = null
   val zookeeper=""
 System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    sc.parallelize(1 to 100).map { x=>
      val b=XContentFactory.jsonBuilder()
      .startObject()
      .field("firstName", x)
      .field("map", Map("age"->1,"age2"->2).asJava)
      .endObject()
      (x/10,b)
    }.foreach { case((d,b))=>println(d) }
    
  }
         
    def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
  
}