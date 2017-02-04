package com.spark.myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
//继承与RDD[String]表示没有前置的rdd。
//这个rdd可以从sc直接获取。而不是一个rdd转换成这个rdd
class MySelfRDD(@transient sc : SparkContext,val strs:Array[String])extends RDD[String](sc,Nil){
  //这个函数是用来计算RDD中每个的分区的数据
  override def compute(split: Partition, context: TaskContext):Iterator[String] ={
    //得到切片的数据
    val splits = split.asInstanceOf[MySelfPartition]
    Array[String](splits.content).toIterator
  }
  //getPartitions函数允许开发者为RDD定义新的分区
override protected def getPartitions: Array[Partition] ={
   val array = new Array[Partition](strs.size)
    for (i <- 0 until strs.size) {
      array(i) = new MySelfPartition(i, strs(i))
    }
    array
}
}

class MySelfRDD2(parent:RDD[String],data:String)extends RDD[String](parent){
  //这个函数是用来计算RDD中每个的分区的数据
  override def compute(split: Partition, context: TaskContext):Iterator[String] ={
    //得到切片的数据
    parent.iterator(split, context).map { x => data+x }
  }
  //getPartitions函数允许开发者为RDD定义新的分区
override protected def getPartitions: Array[Partition] =
      parent.partitions
}

class MySelfPartition(idx: Int, val content: String) extends Partition {
  override def index: Int = idx
}