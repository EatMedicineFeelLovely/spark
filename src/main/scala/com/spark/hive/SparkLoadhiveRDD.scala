package com.spark.hive
import org.apache.hadoop.io.{Writable}
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema
import org.apache.hive.hcatalog.data.schema.HCatSchema
import org.apache.hive.hcatalog.mapreduce
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat

import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions.asScalaBuffer
object SparkLoadhiveRDD {
  def main(args: Array[String]): Unit = {
    setHiveConf
    val sparkContext=new SparkContext()
 val f = classOf[HCatInputFormat]
    val k = classOf[org.apache.spark.SerializableWritable[org.apache.hadoop.io.Writable]]
    val v = classOf[org.apache.hive.hcatalog.data.HCatRecord]
    HCatInputFormat.setInput(sparkContext.hadoopConfiguration, "test", "mb_conv_test")
   /*val d= sparkContext.newAPIHadoopRDD(
       sparkContext.hadoopConfiguration, f, k, v)*/
    
    
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
