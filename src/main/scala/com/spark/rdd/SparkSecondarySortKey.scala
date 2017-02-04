package com.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.HashSet
import java.util.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.Partitioner
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.WritableComparator
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import scala.reflect.ClassTag
object SparkSecondarySortKey {
  var sc: SparkContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    val sd=new SecondarySortKey("a",1)
    val ds=new SecondarySortKey("a",2)
    println(sd.equals(ds))
   val a=Array(("a",1),("a",9),("b",4),("o",7),("b",9),
       ("b",3),("f",4),("k",8),
       ("a",15),("z",4),("b",1))
   val rdd=sc.parallelize(a)
  //实现二次排序：先按first字段排序，然后按second排序
   val hrdd=rdd.map { case(first,second) => 
     val key=new SecondarySortKey(first,second)
     (key,second)
    }.groupByKey()
     .map{x=>(x._1,x._2.toList.sorted)}
     .sortByKey()
     .foreach(println)
}
  def init() {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
  
  /**
   * 自定义分区
   */
  class IteblogPartitioner(override val numPartitions: Int) extends Partitioner {
  //override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    val first = key.asInstanceOf[SecondarySortKey].first
    val code = (first.hashCode % numPartitions)
    if (code<0) {
      code+numPartitions
    } else {
      code
    }
  }
  override def equals(other: Any): Boolean = other match {
    case iteblog: IteblogPartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }
  override def hashCode: Int = numPartitions
}
  /**
   * 自定义一个key
   */
class SecondarySortKey(var first:String,var second:Int) 
  extends WritableComparable[SecondarySortKey] with Serializable{
  def set(left:String,right:Int) {
    first = left;
    second = right;
  }
  def getFirst()=first
  def getSecond() =second
 override  def readFields(in:DataInput){
    first = in.readUTF();
    second = in.readInt();
  }
  override def write(out:DataOutput){
    out.writeUTF(first);
    out.writeInt(second);
}
  override def hashCode() =first.hashCode()
  //这个是在reduce的时候决定哪些key要分配在一起的
  override def equals(right:Any) ={
    if (right.isInstanceOf[SecondarySortKey]) {
      var r = right.asInstanceOf[SecondarySortKey]
      r.first == first
    } else {
      false
    }
  }
  //这里的代码是关键，因为对key排序时  
  def compareTo(o:SecondarySortKey) ={
    if (first != o.first) {
      first.compareTo(o.first)
    } else if (second != o.second) {
      second - o.second
    } else {
      0
    }
  }
  override def toString()={
    first
  }
}
}