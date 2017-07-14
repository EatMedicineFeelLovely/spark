package com.test

import java.util.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Seq
import java.io.File
import java.net.URLClassLoader
import java.net.URL
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import org.json.JSONObject
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import java.security.MessageDigest
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.hadoop.hbase.util.Bytes
import java.util.Random
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Test extends Utilities {
var a = new testcas
  def main(args: Array[String]): Unit = {
    //println(fun((1,1)))
    //val a=new HashMap[String,String]
    //a.put("a", "a")
    //t1(a)
    //println(a)
    //t2(a)
    //println(a)
    /* val url=new File("C:\\Users\\zhiziyun\\Desktop\\test-0.0.1-SNAPSHOT.jar").toURI().toURL()
    val d=new URLClassLoader(Array(url), Thread.currentThread().getContextClassLoader())
   val a= d.loadClass("test.HelloWord")
   a.getMethod("printwoed",classOf[String]).invoke(a.newInstance(),"hello world")
   */
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("s"))
    val rdd = sc.parallelize(Array(6, 8, 10, 11), 4)
    
    //a.setaa("2")
    rdd.map { x => println(a.a) }.count
  }
  class testcas() {
    var a = "1"
    def setaa(a: String) {
      this.a = a
    }
  }
  def t1(a: HashMap[String, String]) {
    a.clear()
  }

  def t2(a: HashMap[String, String]) {
    a.put("1", "1")
  }
  def fun(str: Any, data: String) = {
    str match {
      case i: Int    => "INt" + ":" + data
      case s: String => "String" + ":" + data
      case map: HashMap[_, _] =>
        "Map" + ":" + data
        str.asInstanceOf[HashMap[String, String]].toString()
      case t: (_, _) =>
        "Tuple2" + ":" + data
        t.asInstanceOf[Tuple2[Int, Int]].toString()
    }
  }
  def write(
    data: String,
    fun: (Any, String) => String) = {
    println(fun("", data))
    println(fun((1, 1), data))
  }
}

case class casetest(a: String)(val b: String) {
  def d = {
    println(b)
  }
}
object EnumerationTest extends Enumeration {
  type EnumerationTest = Value
  val b, c, d = Value
}