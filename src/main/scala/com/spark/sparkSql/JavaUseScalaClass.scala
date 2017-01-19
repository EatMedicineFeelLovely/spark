package com.spark.sparkSql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.ArrayList
import java.util.Map
import java.util.List
import com.spark.sparkSql.CaseClassUtil._
import org.apache.spark.api.java.JavaRDD

//在sc.parallelize(data)中的data是一个scala的集合，如果放入java的集合(ArrayList)的话会报错，
//加入import scala.collection.JavaConversions._就不会报错了,内部会自己转换
class JavaUseScalaClass(sc:SparkContext,sqlContext:SQLContext) {
   def userRDDToDataFrame(data:ArrayList[HashMap[String,String]],tableName:String){
    var liens=sc.parallelize(data).map(t=>User(name=t.get("name"),age=t.get("age").toInt,phone=t.get("phone")))
    sqlContext.createDataFrame(liens).registerTempTable(tableName)
  }

  def addressRDDToFrame(data:ArrayList[HashMap[String,String]],tableName:String){
    var liens=sc.parallelize(data).map(t=>Address(name=t.get("name"),t.get("address"),phone=t.get("phone")))
    sqlContext.createDataFrame(liens)registerTempTable(tableName)
  }
  //第二种指定Schema,需要这个ROW
  def secondRDDToFrame(data:ArrayList[HashMap[String,String]]){
    var liens=sc.parallelize(data).map(p=>Row(p.get("name"),p.get("phone")))
    var schemaString = "name phone"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    sqlContext.createDataFrame(liens, schema)registerTempTable("Detail")
  }
  def show(sql:String):List[Row]={
    var data=sqlContext.sql(sql)
    
    data.show()
    return data.collectAsList()
  }
}  
