package com.spark.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Test {
	System.setProperty("hadoop.home.dir", "E:\\eclipse\\hdplocal2.6.0")
  var conf = new SparkConf()
    .setMaster("local")
    .setAppName("Spark Pi")
  var sc = new SparkContext(conf)
  val spark = new SQLContext(sc)
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val data = sc.parallelize(Array[String]("1","2"))
    val df=data.map{x=>test(Array(1,2),x)}.toDF()
    df.registerTempTable("")
    
    
    
  }

  case class test(arr: Array[Int], name: String)
}