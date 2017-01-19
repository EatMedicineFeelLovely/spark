package com.spark.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat._
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.hive.hcatalog.data.schema.HCatSchema
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
import org.apache.hadoop.io.NullWritable
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
object SparkToHive {
	System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
	case class User2(name:Int,age:Int,sex:Int)
  var hiveconf = new SparkConf().setAppName("sparkhivetest").setMaster("local")
        setHiveConf
  val  sc = new SparkContext(hiveconf)
  val  sqlContext = new HiveContext(sc)
  import  sqlContext.implicits._
  def main(args: Array[String]): Unit = {
    //useHCatOutputFormatToHive
    //secondRDDToFrame
    insertintoHive
    //readHiveData
    //creatTable
    
  }
  def creatTable(){
    sqlContext.sql("use test1")
    sqlContext.sql("create table test_creat(id int,order_id int,product_id int) row format delimited fields terminated by ','STORED AS TEXTFILE")
  }
  def readHiveData() {
    sqlContext.sql("use default")
    sqlContext.sql("select * from siteorderlog limit 10").show
    sc.stop()
  }
  def  insertintoHive(){
     var rdd=sc.parallelize(Array(Map("name"->3,"age"->4,"sex"->5)))
                 .map{x=>User2(name=x("name"),age=x("age"),sex=x("sex"))}
    //方法1
     //import  sqlContext.implicits._
     //rdd.toDF().registerTempTable("user2")
    //方法2
     sqlContext.createDataFrame(rdd).registerTempTable("user2")
     sqlContext.sql("select * from user2").show
     
     sqlContext.sql("insert into table test1.test_creat "+ 
                    "select name,age,sex from user2")
        
  }
    //第二种指定Schema,需要这个ROW
  def secondRDDToFrame(){
    var arraybuffer=ArrayBuffer[HashMap[String,Int]]()
    var map=new HashMap[String,Int]()
    map.put("name", 1)
    map.put("age", 1)
    map.put("sex", 1)
    arraybuffer+=map
    var liens=sc.parallelize(arraybuffer)
              .map(p=>Row(p.get("name"),p.get("age"),p.get("sex")))
    var schemaString = Array("name","age","sex")
    var columns=schemaString.map(fieldName => StructField(fieldName, IntegerType, true))
    val schema = StructType(columns)
    var schemaData=sqlContext.createDataFrame(liens, schema)
    schemaData.registerTempTable("user2")
    sqlContext.sql("select * from user2").show()
    sqlContext.sql("insert overwrite  table test1.test_creat select  name,age,sex from user2")
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
