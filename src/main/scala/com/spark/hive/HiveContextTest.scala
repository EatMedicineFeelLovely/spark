package com.spark.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame

object HiveContextTest {
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
	case class User2(name:Int,age:Int,sex:Int)
  var hiveconf = new SparkConf().setAppName("sparkhivetest").setMaster("local")
  setHiveConf
  val  sc = new SparkContext(hiveconf)
  val  sqlContext = new HiveContext(sc)
  def main(args: Array[String]): Unit = {
    sqlContext.sql("use test")
    val df=sqlContext.sql("select * from test.mb_conv_test")
    df.rdd.collect().foreach { x=>println(">>>>>>>>>>>>>>>>> "+x) }
    
   /* var rdd=sc.parallelize(Array(Map("name"->1,"age"->2,"sex"->3))).map{x=>User2(name=x("name"),age=x("age"),sex=x("sex"))}
    sqlContext.createDataFrame(rdd).registerTempTable("user2")
    sqlContext.sql("show tables").show
    sc.stop()*/
  }
  def setHiveConf() {
    //加一下的信息，就可以不用使用hive-site.xml和hdfs-site.xml了
    //信息在/etc/hive/conf/hive-site.xml里面
     //加配置文件是最保险的。有时候加下面的也不成功
    System.setProperty("hive.metastore.uris", "thrift://mongodb3:9083")
    System.setProperty("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    System.setProperty("hive.zookeeper.quorum", "mongodb3,solr2,solr1")
    System.setProperty("hive.zookeeper.client.port", "2181")
    
    System.setProperty("dfs.nameservices", "nameservice-zzy")
    System.setProperty("dfs.client.failover.proxy.provider.nameservice-zzy", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    System.setProperty("dfs.ha.automatic-failover.enabled.nameservice-zzy", "true")
    System.setProperty("ha.zookeeper.quorum", "mongodb3:2181,solr1:2181,solr2:2181")
    System.setProperty("dfs.ha.namenodes.nameservice-zzy", "namenode47,namenode237")
    System.setProperty("dfs.namenode.rpc-address.nameservice-zzy.namenode47", "mongodb3:8020")
    System.setProperty("dfs.namenode.servicerpc-address.nameservice-zzy.namenode47", "mongodb3:8022")
    System.setProperty("dfs.namenode.http-address.nameservice-zzy.namenode47", "mongodb3:50070")
    System.setProperty("dfs.namenode.https-address.nameservice-zzy.namenode47", "mongodb3:50470")
    System.setProperty("dfs.namenode.rpc-address.nameservice-zzy.namenode237", "solr2:8020")
    System.setProperty("dfs.namenode.servicerpc-address.nameservice-zzy.namenode237", "solr2:8022")
    System.setProperty("dfs.namenode.http-address.nameservice-zzy.namenode237", "solr2:50070")
    System.setProperty("dfs.namenode.https-address.nameservice-zzy.namenode237", "solr2:50470")
    System.setProperty("dfs.namenode.http-address.nameservice-zzy.namenode47", "mongodb3:50070")
    System.setProperty("dfs.client.use.datanode.hostname", "false")
    
    System.setProperty("fs.permissions.umask-mode", "022")
    System.setProperty("dfs.namenode.acls.enabled", "false")
    System.setProperty("dfs.client.read.shortcircuit", "false")
    System.setProperty("dfs.namenode.acls.enabled", "false")
    System.setProperty("dfs.domain.socket.path", "/var/run/hdfs-sockets/dn")
    System.setProperty("dfs.client.read.shortcircuit.skip.checksum", "false")
    System.setProperty("dfs.client.domain.socket.data.traffic", "false")
    System.setProperty("dfs.datanode.hdfs-blocks-metadata.enabled", "true")
    
    
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
