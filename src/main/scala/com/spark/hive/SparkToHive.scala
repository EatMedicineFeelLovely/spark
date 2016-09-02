package com.spark.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat._
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
import org.apache.hive.hcatalog.data.schema.HCatSchema
import org.apache.hadoop.io.NullWritable
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat

object SparkToHive {
  var sc: SparkContext = null
  var sqlContext: HiveContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    useHCatOutputFormatToHive
    //readHiveData
    //creatTable
  }
  def creatTable(){
    sqlContext.sql("use test")
    sqlContext.sql("create table test_creat(id int,order_id int,product_id int) row format delimited fields terminated by ','STORED AS TEXTFILE")
  }
  def readHiveData() {
    var siteuserlog = sqlContext.sql("select * from siteuserlog limit 10").rdd
    siteuserlog.map { x => ">>>"+x }.foreach { println }
    println(">>>>>>>>>>>>>>>>>>>>..e")
    //siteuserlog.foreach { println }
    sc.stop()
  }
  def setHiveConf() {
    //加一下的信息，就可以不用使用hive-site.xml了
    System.setProperty("hive.metastore.uris", "thrift://Virtual-1:9083")
    System.setProperty("hive.metastore.client.socket.timeout", "300")
    System.setProperty("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    System.setProperty("hive.warehouse.subdir.inherit.perms", "true")
    System.setProperty("hive.enable.spark.execution.engine", "false")
    System.setProperty("hive.zookeeper.quorum", "Virtual-4,Virtual-3,Virtual-2")
    System.setProperty("hive.zookeeper.client.port", "2181")
    System.setProperty("hive.cluster.delegation.token.store.class", "org.apache.hadoop.hive.thrift.MemoryTokenStore")
    System.setProperty("hive.server2.enable.doAs", "true")
    System.setProperty("hive.metastore.execute.setugi", "true")
    System.setProperty("hive.support.concurrency", "true")
    System.setProperty("hive.zookeeper.namespace", "hive_zookeeper_namespace_hive")
    System.setProperty("hive.server2.use.SSL", "false")
    System.setProperty("hive.conf.restricted.list", "hive.enable.spark.execution.engine")
  }
  def init() {
    var hiveconf = new SparkConf().setAppName("sparkhivetest").setMaster("local")
    setHiveConf
    sc = new SparkContext(hiveconf)
    sqlContext = new HiveContext(sc)

  }
  def useHCatOutputFormatToHive() {
    var a = sc.parallelize(Array(("test", 1), ("test2", 2), ("test3", 3), ("test4", 4)))
    var job = Job.getInstance();
    HCatOutputFormat.setOutput(job, OutputJobInfo.create("test", "test", null));
    var recordSchema = getTableSchema(job.getConfiguration())
    HCatOutputFormat.setSchema(job, recordSchema)
    job.setOutputFormatClass(classOf[HCatOutputFormat])
    job.setOutputKeyClass(classOf[NullWritable]);
    job.setOutputValueClass(classOf[DefaultHCatRecord]);
    var jobconf = job.getConfiguration
    var c = a.map { x =>
      var record = new DefaultHCatRecord(recordSchema.size());
      record.setString("name", recordSchema, x._1)
      record.setString("age", recordSchema, x._2.toString)
      (NullWritable.get(), record)
    }
    c.saveAsNewAPIHadoopDataset(jobconf)

  }
}