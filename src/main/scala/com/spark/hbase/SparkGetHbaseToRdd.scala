package com.spark.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SparkReadMoreFiles {
  var sc: SparkContext = null
  def main(args: Array[String]): Unit = {
    init
//HCatOutputFormat
    var conf = sc.hadoopConfiguration
    conf.set(TableOutputFormat.OUTPUT_TABLE, "test")
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "Virtual-1,Virtual-2,Virtual-3")
    sc.hadoopConfiguration.set("zookeeper.znode.parent", "/hbase")
    var job = new Job(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    conf = job.getConfiguration
    for (i <- 1 to 100) {
      println(i)
      var a = sc.parallelize(i*100000 to (i+1)*(100000))
      var b = a.map { x =>
        println(x)
        var p = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(x))))
        p.addColumn("info".getBytes, "test".getBytes, Bytes.toBytes(x))
        (new ImmutableBytesWritable, p)
      }
        .saveAsNewAPIHadoopDataset(conf)
    }
  }
  def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
}