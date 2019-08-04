package com.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.ui.StreamingJobProgressListener

object Test {
  def main(args: Array[String]): Unit = {
    val con=new SparkConf().setMaster("local").setAppName("aa")
    val sc=new SparkContext(con)
    val ssc=new StreamingContext(sc,Seconds(1))
    ssc.addStreamingListener(new SelfStreamingListener)//默认有一个StreamingJobProgressListener
    sc.addSparkListener(new SelfSparkListener) 
    val rdd=sc.parallelize(1 to 100)
    rdd.foreach { println }
    
  }
}