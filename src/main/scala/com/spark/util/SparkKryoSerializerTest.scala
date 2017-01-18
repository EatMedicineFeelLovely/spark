package com.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkKryoSerializerTest {
  var sparkconf:SparkConf=null
  var sc:SparkContext=null
  def main(args: Array[String]): Unit = {
    sparkInit
    testKryoSerializer
  }
def testKryoSerializer{
  var personList = 1 to 10 map (value => new MygisterKryoClass(value + ""))
  var myrdd= sc.parallelize(personList)
  myrdd.foreach { x=>println(x.getName) }
}
   
def sparkInit(){
    sparkconf = new SparkConf()
    .setMaster("local")
    .setAppName("Spark Pi")
		sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
		sparkconf.set("spark.kryo.registrator", "com.spark.util.SparkKryoRegistrators")
   //sparkconf.registerKryoClasses(Array(classOf[MygisterKryoClass],classOf[String]))
    sc = new SparkContext(sparkconf)
}
class MygisterKryoClass(var name:String){
  //private var name:String=null
   def getName={
    name
  }
  def setName(name:String)={
    this.name=name
  }
}
}