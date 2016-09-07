package com.spark.test

import java.util.HashMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Hashtable
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.HashMap
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object SparkTest {
   var a:Hashtable[String,Int]=new Hashtable[String,Int]()
   var conf:Configuration=null
  var connection:Connection=null
  var sparkconf:SparkConf=null
  var sc:SparkContext=null
   var job:Job=null
  var preData:RDD[(String,(HashMap[String,String],Int))]=null
  def main(args: Array[String]): Unit = {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    init()
  
    var data=sc.parallelize(Array(("c",1),("b",2)),3)
    var a=data.map(x=>{println(">>");(x._1,x._2+100)})
    a.cache()
    a.collect().foreach(println)
    a.collect().foreach(println)
    
   //preData=changePreData(result)
  //需要进hbase的数据
  //preData.foreach(println)
  //preData.filter(x=>x._2._2==1).foreach(println)
  
  //var data2=sc.parallelize(Array(("c",1),("b",2)),3)
  //result.collect().foreach(println)
  
  
  }
   def init(){
    var pre:HashMap[String,HashMap[String,String]]=new HashMap[String,HashMap[String,String]]()
    var d=new HashMap[String,String]()
    d.put("clicks", "22")
    d.put("clickUV", "12314")
    pre.put("a", d)
    pre.put("c",d)
    sparkconf = new SparkConf().setMaster("local").setAppName("Spark Pi")
    sc = new SparkContext(sparkconf)
    
    /*preData=sc.parallelize(pre.toSeq, 3).map(x=>(x._1,(x._2,1)))
    //初始化化hbase的配置
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "test")
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ","Virtual-1,Virtual-2,Virtual-3")
    sc.hadoopConfiguration.set("zookeeper.znode.parent","/hbase")
    job = new Job(sc.hadoopConfiguration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
     //streaming从这里开始  
  var data=sc.parallelize(Array(("a",1),("b",2)),3)
  //将旧数据和新数据进行join
  var result=data.fullOuterJoin(preData)
  smartAdsLogClick(result)*/
   }
   def smartAdsLogClick(result:RDD[(String, (Option[Int], Option[(HashMap[String, String], Int)]))]){
     preData=changePreData(result)
     //过滤出需要进hbase的数据,1是表示要进的
     //preData.filter(x=>x._2._2==1).collect().foreach(println)
     preData.foreach(println)
      preData.filter(x=>x._2._2==1)
     .map(changeDataToPut)
     .saveAsNewAPIHadoopDataset(job.getConfiguration)
   }
   def changeDataToPut(data:((String, (HashMap[String,String], Int))) )={
     val p = new Put(data._1.getBytes)
      p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("test"),data._2._1.get("clicks").getBytes)
      (new ImmutableBytesWritable, p)
   }
   def changePreData(joinresult:RDD[(String, (Option[Int], Option[(HashMap[String, String], Int)]))])={
     joinresult.map(x=>{
    var rowkey=x._1
    var data=x._2
    var newdata=data._1
    var predata=data._2
    var d=new HashMap[String,String]()
    var flag=1//1表示数据有变化需要进hbase
    if(predata.isDefined){
      //判断rowkey存在
      d=predata.get._1
    if(newdata.isDefined){
      if(d.get("clicks")==null)//字段不存在
         d.put("clicks", newdata.get.toString)
      else 
        d.put("clicks", (d.get("clicks").toInt+newdata.get).toString())
      }else{
        flag=0
      }
    }else{
      d.put("clicks",newdata.get.toString)
    }
    (rowkey,(d,flag))
  })
   }
/* def putDataToHabse(){
   sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "test2")
    //hbaseInitialize()
    a.put("a", 233)
//指定输出格式和输出表名
  val job = new Job(sc.hadoopConfiguration)
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    sc.parallelize(Array(1,2,3),3).map(convert).saveAsNewAPIHadoopDataset(job.getConfiguration)
 }*/
def convert(triple: Int) = {
      val p = new Put(Bytes.toBytes(triple))
      p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(triple))
      (new ImmutableBytesWritable, p)
} 
def hbaseInitialize(){
    conf=HBaseConfiguration.create()
    /*conf.set("hbase.zookeeper.quorum", "Virtual-1,Virtual-2,Virtual-3");
		conf.set("hbase.zookeeper.property.clientPort", "2181");*/
    connection=ConnectionFactory.createConnection(conf);
  }
/*def saveGameLogToHBase(rdd: RDD[String], zkQuorum: String, tableName: String): Unit = {
     val conf = HBaseConfiguration.create()
     conf.set("hbase.zookeeper.quorum", zkQuorum)
      //设置表名,如果不是原始日志
    val columnAndvalueMap = Map[String, String]()
    new PairRDDFunctions(rdd.map { line =>
      //生成rowkey
        //val rowkey = HBaseUtils.createRowKey(line, tableName)
      val rowkey = HBaseUtils.buildUUID
        //val tablename = "n2_outer_"+line.split("#")(0)+"_log"
        //conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
        gameLogToMap("#", line, columnAndvalueMap)
        val tablebname = line.split("#")(0)

      createHBaseRecord(LogFormatBean.CF, rowkey, columnAndvalueMap,tablebname)
  }).saveAsNewAPIHadoopFile("",classOf[String] , classOf[String], classOf[MultiTableOutputFormat], conf)
  }*/
}
