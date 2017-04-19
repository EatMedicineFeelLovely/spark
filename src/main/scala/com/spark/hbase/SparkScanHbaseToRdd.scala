package com.spark.hbase

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import java.util.ArrayList
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import java.util.HashMap
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
object SparkScanHbaseToRdd {
  var sc: SparkContext = null
  var conf: Configuration = null
  def main(args: Array[String]): Unit = {
		System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
    var tableName = "rt_rtbreport"
    var zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
    var scans = new Scan
    var filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*2016-10-20"))
    scans.setFilter(filter)
    val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("HBaseDistributedScanExample")
    sc = new SparkContext(sparkConf)
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeper)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.addResource(new Path("conf/core-site.xml"))
    //conf.addResource(new Path("conf/hbase-site.xml"))
    //conf.addResource(new Path("conf/hdfs-site.xml"))
   
    var a = hbaseRDD2[(String, HashMap[String, String])](
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => {
      var rowMap = new HashMap[String, String]()
      var listCells = r._2.listCells()
      val rowkey = Bytes.toString(r._2.getRow)
      for (cell <- listCells) {
        var column = new String(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        rowMap.put(column, new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
    	}
      (rowkey, rowMap)
      })
    println(a.partitions.size)
    a.foreach(println)

  }
  def hbaseRDD[U: ClassTag](tableName: String, scan: Scan, f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    var job: Job = new Job(conf)
    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
  def hbaseRDD2[U: ClassTag](tableName: String, scan: Scan, f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {
    var proto = ProtobufUtil.toScan(scan);
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()))
    var job: Job = new Job(conf)
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
}