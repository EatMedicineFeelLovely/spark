package com.spark.sparkSql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import scala._
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import scala.collection.mutable.HashMap
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.HBaseConfiguration
object SparkSQLDemo {
def main(args: Array[String]): Unit = {
  var conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("Spark Pi")
System.setProperty("hadoop.home.dir", "E:\\eclipse\\hdplocal2.6.0")
    var sc = new SparkContext(conf)
  
    var sql=new SQLContext(sc)
    
  testDataFram(sc,sql)
  
  
 
}
def testDataFram(sc:SparkContext,sql:SQLContext){
    val data=sc.textFile("F:\\data\\smartadsclicklog")
    val fram=data.map { x => {x.split(",")}}.map { x =>Smartadsclicklog(
           clicktime=x(0),zzid=x(1),siteid=x(2),uid=x(3),
           ip=x(4),originurl=x(5),pageurl=x(6),campaign=x(7),
           template=x(8),pubdomain=x(9),visitor=x(10),useragent=x(11),
           slot=x(12),unit=x(13),creative=x(14),ext=x(15),
           bidid=x(16)) }
    val df=sql.createDataFrame(fram)
    df.rdd.foreach{row=>
      
    println(row(16))
    }
    //df.registerTempTable("Smartadsclicklog")
    //sql.sql("select * from Smartadsclicklog").show()
    println(">>>>>>>>>>>>>>>>>..")
  }
/*def transStrToPut(row:Row,cols:Array[String])={
      val r=cols.zip(row.toSeq)
      r.map{case(colname,value)=>
      val put=new Put()  
      
      }
      val put = new Put(cells(0).getBytes);
      put.addColumn(cells(0).getBytes, cells(0).getBytes, cells(0).getBytes)
      put
    }*/
  case class Smartadsclicklog(clicktime:String,zzid:String,siteid:String,uid:String,
    ip:String,originurl:String,pageurl:String,campaign:String,
    template:String,pubdomain:String,visitor:String,useragent:String,
    slot:String,unit:String,creative:String,ext:String,bidid:String)
}
case class HBaseRecord(
  col0: String,
  col1: Int)

object HBaseRecord {
  def apply(i: Int, t: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i)
  }
}