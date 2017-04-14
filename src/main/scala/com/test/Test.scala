package com.test

import java.util.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Seq
import java.io.File
import java.net.URLClassLoader
import java.net.URL
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.test.Utilities
object Test extends Utilities{
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
    val v_l5mon_date = getDateStr(getNMonthAgo(getNDayAgo(1), 4))
    println(v_l5mon_date)
    val v_data_date = getDateStr_(getNDayAgo(1))
    println(v_data_date)
    val v_next_date = getDateStr_()
    println(v_next_date)
    val v_data_day = getDateStr_()
    println(v_data_day)
    val v_mth_stt = getMonthStart()
    println(v_mth_stt)
    val v_mth_end = getMonthEnd()
    println(v_mth_end)
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