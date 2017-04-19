package com.spark.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import scala.collection.JavaConversions._
import java.util.ArrayList
import java.util.Map
import java.util.List
import com.spark.sparkSql.CaseClassUtil._
object SparkListToDataFrame {
    var conf = new SparkConf() .setMaster("local").setAppName("Spark Pi")
    var sc = new SparkContext(conf)
    var sqlContext=new SQLContext(sc)
    System.setProperty("hadoop.home.dir", "E:\\eclipse\\hdplocal2.6.0")
   //第一种，使用反射
    def main(args: Array[String]): Unit = {
      //secondRDDToFrame()
      //show("select * from Detail")
      AddressRDDToFrame
    }
  def UserRDDToDataFrame(data:ArrayList[HashMap[String,String]],tableName:String){
    //这是java的写法
    var liens=sc.parallelize(data).map(t=>User(name=t.get("name"),age=t.get("age").toInt,phone=t.get("phone")))
    var userData=sqlContext.createDataFrame(liens,User.getClass)
    userData.registerTempTable(tableName)
  }
  def AddressRDDToFrame(){
    var arraybuffer=ArrayBuffer[HashMap[String,String]]()
    var map=new HashMap[String,String]()
    map.put("name", "lmq")
    map.put("address", "莆田")
    arraybuffer+=map
    var liens=sc.parallelize(arraybuffer)
    .map(t=>Address(name=t.get("address"),t.get("address"),phone=t.get("address")))
    var addressData=sqlContext.createDataFrame(liens)
    addressData.registerTempTable("Address")
    show("select * from Address")
    
    var liens2=sc.parallelize(arraybuffer).map(t=>Address(name=t.get("name"),t.get("name"),phone=t.get("name")))
    var addressData2=sqlContext.createDataFrame(liens2)
    addressData2.registerTempTable("Address")
    
    show("select * from Address")
    
  }
  //第二种指定Schema,需要这个ROW
  def secondRDDToFrame(){
    var arraybuffer=ArrayBuffer[HashMap[String,String]]()
    var map=new HashMap[String,String]()
    map.put("name", "lmq")
    map.put("age", "12")
    map.put("phone", "10312123")
    arraybuffer+=map
    var liens=sc.parallelize(arraybuffer)
              .map{p=>val r=Row(p.get("name"),p.get("phone"),p.get("age"));
              Row(Array(1,2))
              r
  }
    var schemaString = Array("name","phone","age")
    val types=Array(StringType,IntegerType,DoubleType)
    var columns=schemaString.zip(types).map{case(name,tp)=>
      StructField(name, "", true)
    }
    val schema = StructType(columns)
    var schemaData=sqlContext.createDataFrame(liens, schema)
    schemaData.registerTempTable("Detail")
    
    
  }
  def show(sql:String){
    sqlContext.sql(sql).show()
  }
  implicit def strToStringType(str:String):DataType={
    str match {
      case "String" => StringType
      case "Int" => IntegerType
      case _ => StringType
    }
    
  }
}  


    
    
    
    
    
    