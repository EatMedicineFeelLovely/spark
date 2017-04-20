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
import org.apache.spark.sql.types.IntegerType
object SparkSQLDemo {
def main(args: Array[String]): Unit = {
  var conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("Spark Pi")
System.setProperty("hadoop.home.dir", "E:\\eclipse\\hdplocal2.6.0")
    var sc = new SparkContext(conf)
    val spark=new SQLContext(sc)
    import spark.implicits._
    val data=sc.parallelize(Array[String]())
    spark.udf.register[Int,Int,Int]("udf1", udf1(_:Int,_:Int))
    spark.udf.register[String,String,String]("udf2", udf2(_:String,_:String))
    data.map {x=>HBaseRecord(x,1) }
    .toDF
    .registerTempTable("test")
    
    val data2=sc.parallelize(Array[String]("a","b"))
    data2.map {x=>HBaseRecord(x,1) }
    .toDF
    .registerTempTable("test_tmp")
    //IF(a.col1 is NULL,b.col1,IF(b.col1 is null,a.col1,a.col1+b.col1)) 
    spark.sql(""" 
      select udf2(a.col0,b.col0) as col0,
      case when a.col1 is null then b.col1
           when b.col1 is null then a.col1
           else a.col1+b.col1 end
           as col1
      from  
      test a full join test_tmp b 
      on a.col0=b.col0
      """).registerTempTable("test")
    spark.sql("select * from test").show
  //testDataFram(sc,spark)
  
  
    
 
}
def udf1(acol1:Int,bcol1:Int)={
  bcol1 
}
def udf2(acol0:String,bcol0:String)={
  if(acol0!=null){
    acol0
  }else bcol0
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