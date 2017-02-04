package com.spark.jdbcrdd

import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.ResultSet

object SparkJdbcRDDTest {
  def main(args: Array[String]) {  
    val sc = new SparkContext("local","spark_mysql")  
    val numPartitions=10
    val sql="select * from zz_reporting.st_rtbreport_byplan where StatDate='2016-05-01'"
    //限制：会出现数据丢失和数据重复的现象，因为你在取数据的时候，会出现数据删除和数据添加的情况，
    //这样数据的顺序就会打乱，
    //使用自带的JdbcRDD可以解决数据丢失 的问题，但是限制性比较大
    
    val data=sc.mysqlRDD(createConnection, sql, numPartitions, extractValues)
    data.printlnRDD
    
    sc.stop()  
  } 
 
}