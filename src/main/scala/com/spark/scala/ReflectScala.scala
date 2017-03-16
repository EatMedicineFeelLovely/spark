package com.spark.scala

import java.io.File
import java.net.URLClassLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

object ReflectScala {
  def main(args: Array[String]): Unit = {
    val fs = FileSystem.get(new Configuration)
    
    val dsf="/user/linmingqiang/text.txt"
    val hdfsInStream = fs.open(new Path(dsf));  
    val a=Array[Byte]()
hdfsInStream.readFully(a)

  }
  /**
   * 动态加载jar包
   */
  def classLoaderTest() {
    //如果jar包不是在项目里，或者说jar包是在程序运行中要加进来，但是程序不重新编译。
    val url = new File("C:\\Users\\zhiziyun\\Desktop\\test-0.0.1-SNAPSHOT.jar").toURI().toURL()
    val d = new URLClassLoader(Array(url), Thread.currentThread().getContextClassLoader())
    val a = d.loadClass("test.HelloWord")
    //var a = Class.forName("com.test.ReflectScala")
    //因为该方法是一个静态的方法，所以这个地方的invoke只要填null就可以了。但是如果不是一个静态方法，就需要一个实例
    //a.getMethod("test").invoke(a.newInstance())
    a.getMethod("test", classOf[String]).invoke(null, "hello world")
  }

}