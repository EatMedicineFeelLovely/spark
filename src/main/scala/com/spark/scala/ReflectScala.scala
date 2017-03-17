package com.spark.scala

import java.io.File
import java.net.URLClassLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import java.net.URL
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

object ReflectScala {
  def main(args: Array[String]): Unit = {
    loadHdfsJar

  }
  /**
   * 动态加载jar包
   */
  def loadHdfsJar() {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    val fs = FileSystem.get(new Configuration)
    val jarPath = "/user/linmingqiang/test-0.0.1-SNAPSHOT.jar"
    val url = fs.getFileStatus(new Path(jarPath)).getPath().toUri().toURL()
    val d = new URLClassLoader(Array(url), Thread.currentThread().getContextClassLoader())
    val a = d.loadClass("test.HelloWord")
    //因为该方法是一个静态的方法，所以这个地方的invoke只要填null就可以了。但是如果不是一个静态方法，就需要一个实例a.newInstance()
    //a.getMethod("printwoed", classOf[String]).invoke(null, "hello world")
    a.getMethod("printwoed", classOf[String]).invoke(a.newInstance(), "hello world")
  }
  def loadLocalJar() {
    val url = new File("C:\\Users\\zhiziyun\\Desktop\\test-0.0.1-SNAPSHOT.jar").toURI().toURL()
    val d = new URLClassLoader(Array(url), Thread.currentThread().getContextClassLoader())
    val a = d.loadClass("test.HelloWord")
    //因为该方法是一个静态的方法，所以这个地方的invoke只要填null就可以了。但是如果不是一个静态方法，就需要一个实例
    //a.getMethod("test").invoke(a.newInstance())
    a.getMethod("printwoed", classOf[String]).invoke(a.newInstance(), "hello world")

  }

}