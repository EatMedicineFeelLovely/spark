package com.spark.util

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

class MySelfRDD(prev:RDD[String],data:String)extends RDD[String](prev){
  //这个函数是用来计算RDD中每个的分区的数据
  override def compute(split: Partition, context: TaskContext):Iterator[String] ={
    //得到切片的数据
    prev.iterator(split, context).map { x => data+"@"+x }
  }
  //getPartitions函数允许开发者为RDD定义新的分区
override protected def getPartitions: Array[Partition] =
      prev.partitions
}