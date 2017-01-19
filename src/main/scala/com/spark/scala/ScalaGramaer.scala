package com.spark.scala

import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import java.util.HashMap
import scala.io.Source
import java.io.File
import scala.collection.Iterator
import sun.org.mozilla.javascript.internal.ast.Yield
import scala.reflect.ClassTag
import java.io.PrintWriter
import scala.tools.cmd.Opt.Implicit
object ScalaGramaer {
  var list = new ArrayList[String]
  //implicit val aa="a"
  implicit def testimplicit(implicit i: String) = {
    i.toInt
  }
  implicit def testimplicit2(i: String) = {
    i.toInt
  }
  def main(args: Array[String]): Unit = {
    //listGrammer()
    //mapGrammer()
    //tupleGrammer()
    //IteratorGrammer
    //var b=aas(1,"1",(_+_+_+55))
    // writeFile
    //setGrammer
    //mapResultTest
    println(1.07 < -1.0)
  }
  def ziptest(){
    val l1 = 1 to 10 toList
    val l2 = l1.tail
    l2.foreach { println }
    println(">>>>")
    val l3=l1.zip(l2)
    l3.foreach { println }
    println(">>>>")
    l3.map(p=>((p._2 - p._1),p._2+"-"+p._1)).foreach { println }
  }
def implicitTest(){
    var a: String = "laal"
    var i: Int = a
    println(i)
    var b:Int="as"
    
}
  def mapResultTest() {
    var a = Set(1, 2, 3, 4)
    println(a.+(5))
  }
  def writeFile() {
    var fw = new PrintWriter(new File("test2"))
    fw.write(">>>>>>>>>")
    fw.close()

  }
  def aas[U: ClassTag](key: Int, value: String, a: (Int, String, Int) => U) = {
    a(key, value, key)

  }
  def IteratorGrammer() {
    var a = Array(Array("1", "2"), Array("3", "4"), Array("5", "6")) //不适用tolist的话，就只能遍历一次
    var fun1 = (x: Array[String]) => true
    var c = a.toIterator.filter { fun1 }
    var b = for {
      i <- a.toIterator
      c <- i
      if c > "2"
      if c < "6"
    } yield c
    //b.foreach { println }
    //b.foreach { println }
    c.foreach { println }
  }
  def setGrammer() {
    var a = Array(1, 2, 3, 4)
    var b =
      for {
        i <- a
      } yield { if (i > 2) i + 1 else i }

    for (i <- b)
      println(i)
  }
  /**
   * scala集合操作
   * 1.想要使用java的集合，需要导入
   * 	  import scala.collection.JavaConversions._
   * 		会内部将java的集合转换为scala的集合
   * 2.java的集合和scala的集合不能显式转换，但是可以隐式转换，如，SparkContext.parallelize(data)
   * 		需要的是一个scala的data，但是可以传一个java的集合
   */
  def fileGrammer() {
    // var file=Source.fromFile("D:\\tmp\\input\\smy_biz_dil\\part-m-00000", "utf-8")
    //var file=Source.fromURL("http://www.baidu.com", "utf-8")
    // file.getLines.foreach { println };
    //bian li mulu 
    /*walk(new File("D:\\tmp\\input\\"))
    list.foreach { println }*/

  }

  //遍历路径下所有的文件
  def walk(file: File) {
    if (file.isDirectory()) file.listFiles().foreach(walk) else list.add(file.getPath())
  }
  def readAllfiles(dir: File): Iterator[File] = {
    //scan a dir return all file 
    var child = dir.listFiles().filter { _.isDirectory() }
    child.toIterator ++ child.toIterator.flatMap { readAllfiles _ }
  }
  def listGrammer() {
    //遍历集合,可以有下标无下标
    var list = new ArrayList[String](); list.add("s")
    for (value <- list) println(value)
    for (i <- 0.until(list.length)) println(list(i))
    for (i <- 0 until list.length) println(list(i))

  }
  def mapGrammer() {
    //mutable可变的
    var map = Map("a" -> 1, "b" -> 2)
    println(map("a"))
    //用get返回的是一个option
    println(map.get("b"))
    println(map.get("c"))
    //改变一个key的值
    map("a") = 6
    println(map("a"))
    //新增一个值
    map += "c" -> 3
    println(map("c"))
    //移除一个值
    map -= "c"
    println(map.getOrElse("c", "无这个key"))
    //如果有这个key就返回key的值
    println(map.getOrElse("null", "无这个key"))

    //遍历一个map
    println("遍历一个map")
    for ((k, value) <- map) {
      println(k + ":" + value)
    }
    println("遍历一个map的key")
    for (k <- map.keySet) {
      println(k)
    }

  }
  def tupleGrammer() {
    //元祖类型Tuple可以是多元的
    var tuple1 = (1)
    var tuple2 = ("1", 2)
    var tuple3 = ("1", 2, "3")
    var tuple4 = ("1", 2, "3", 4)
    println(tuple3._3)

  }

  /**
   * @author Administrator
   */
  class Person(n: String) {
    //必须初始化属性
    var name = n;
    var age = 0;
    var address = "";
    //这是一个辅助构造器，scala的构造器必须以另一个构造器为起点，否则报错
    def this(name: String, age: Int) {
      this(name)
      this.age = age
    }
    def this(name: String, age: Int, address: String) {
      this(name, age)
      this.address = address
    }
  }

}

