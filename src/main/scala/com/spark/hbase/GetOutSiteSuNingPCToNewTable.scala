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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
object GetOutSiteSuNingPCToNewTable {
  var sc: SparkContext = null
  var conf: Configuration = null
  var zookeeper = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  def main(args: Array[String]): Unit = {
    val tableName="outsitepctag"
    val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("GetOutSiteSuNingPCToNewTable")
    sc = new SparkContext(sparkConf)
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeper)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    
    var scan = new Scan
    val  scvf = new SingleColumnValueFilter(  
        Bytes.toBytes("info"),   
        Bytes.toBytes("source"),   
        CompareOp.EQUAL,   
        Bytes.toBytes("baidupclog"));  
      scvf.setFilterIfMissing(false);  
      scan.setFilter(scvf)
    
   var a = hbaseRDD2[(String, HashMap[String, String])](
      tableName,
      scan,
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
      println("##### partition num ##### "+a.partitions.size)
      a.foreach(println)
/*      conf.set(TableOutputFormat.OUTPUT_TABLE, "suningpctag")
      val job = new Job(conf)
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      println("##########  数据准备放入 hbase suningpctag ########")
      a.map{x => 
            val p = new Put(Bytes.toBytes(x._1))
            for((key,value)<-x._2){
              p.addColumn("info".getBytes, key.getBytes, value.getBytes)
            }
              (new ImmutableBytesWritable, p)
            }
       .saveAsNewAPIHadoopDataset(job.getConfiguration)
       sc.stop()*/
    println("##########  结束    ########")
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