package com.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.HashMap
import org.apache.spark.rdd.RDD
import com.sun.org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import java.util.ArrayList
import java.io.File
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import org.apache.hadoop.hbase.util.Bytes
import java.util.ArrayList
import java.util.Date
import java.sql.DriverManager
import org.apache.spark.HashPartitioner
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Milliseconds
import org.apache.hadoop.hbase.client.HConnectionManager
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RegexStringComparator
import java.util.Calendar
import java.text.DateFormat
import java.util.Properties
import java.io.FileInputStream
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import java.util.Date
import java.sql.Timestamp
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Get
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
object CheckHbaseDataWithMysql {
  var sparkconf: SparkConf = null
  var sc: SparkContext = null
  var conf: Configuration = null
  var connection: Connection = null
  import java.sql.Connection
  var mysqlconn: Connection = null
  var zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
   def main(args: Array[String]): Unit = {
     System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
     val time="2016-11-09"
     initMysqlConn2
     initHbaseConn
     val map=getMysqlData(time)
     map.foreach(println)
     
   }
  
  
 def getMysqlData(time:String)={  
   val map=new HashMap[String,HashMap[String,Int]]
   var stam = mysqlconn.createStatement()
   val sql="select plan,sum(Delivery),sum(clicks),sum(reach) "+
           "from Sample_Queue s"+
           " where s.sampUpt>'"+time+"' group by plan"
  var result = stam.executeQuery(sql)         
  while(result.next){ 
    val d=new HashMap[String,Int]
    val plan=result.getString(1)
    val delivery=result.getInt(2)
    val clicks=result.getInt(3)
    val reach=result.getInt(4)
    d.put("delivery", delivery)
    d.put("clicks", clicks)
    d.put("reach", reach)
    map.put(plan,d)
  }
   map
 }
 def initMysqlConn2() {
    var user="developer"
    var pass="dev@zhiziyun^)0628"
    var mysqlurl = "jdbc:mysql://192.168.10.66/zz_bidoptimize"
    Class.forName("com.mysql.jdbc.Driver")
    mysqlconn = DriverManager.getConnection(mysqlurl, user, pass)
  }
 def initHbaseConn {
    var hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zookeeper)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    connection = ConnectionFactory.createConnection(hconf)
  } 
}