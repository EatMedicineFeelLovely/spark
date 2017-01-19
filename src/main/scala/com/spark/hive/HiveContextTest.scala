package com.spark.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object HiveContextTest {
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
	case class User2(name:Int,age:Int,sex:Int)
  var hiveconf = new SparkConf().setAppName("sparkhivetest").setMaster("local")
        setHiveConf
  val  sc = new SparkContext(hiveconf)
  val  sqlContext = new HiveContext(sc)
  def main(args: Array[String]): Unit = {
    var rdd=sc.parallelize(Array(Map("name"->1,"age"->2,"sex"->3))).map{x=>User2(name=x("name"),age=x("age"),sex=x("sex"))}
    sqlContext.createDataFrame(rdd).registerTempTable("user2")
    sqlContext.sql("select * from user2").show
    sc.stop()
  }
   def setHiveConf() {
    //加一下的信息，就可以不用使用hive-site.xml了
    //信息在/etc/hive/conf/hive-site.xml里面
    System.setProperty("hive.metastore.uris", "thrift://CDH-Master:9083")
    System.setProperty("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    System.setProperty("hive.zookeeper.quorum", "CDH-Master,Node2,Node1")
    System.setProperty("hive.zookeeper.client.port", "2181")
    
    
    System.setProperty("hive.metastore.client.socket.timeout", "300")
    System.setProperty("hive.warehouse.subdir.inherit.perms", "true")
    System.setProperty("hive.enable.spark.execution.engine", "false")
    System.setProperty("hive.cluster.delegation.token.store.class", "org.apache.hadoop.hive.thrift.MemoryTokenStore")
    System.setProperty("hive.server2.enable.doAs", "true")
    System.setProperty("hive.metastore.execute.setugi", "true")
    System.setProperty("hive.support.concurrency", "true")
    System.setProperty("hive.zookeeper.namespace", "hive_zookeeper_namespace_hive")
    System.setProperty("hive.server2.use.SSL", "false")
    System.setProperty("hive.conf.restricted.list", "hive.enable.spark.execution.engine")
  }
}