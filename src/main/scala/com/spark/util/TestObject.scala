package com.spark.util

import java.util.Hashtable
import java.util.ArrayList
import scala.collection.JavaConversions._
import com.spark.test.SparkTest
object TestObject {
  var pre:Hashtable[String,Int]=SparkTest.a
def test(x:Iterator[Int]):Iterator[Int]={
     var result= new ArrayList[Int]()
     for(s<-x){
       result.add(s+pre.get("a"))
     }
     result.iterator()
   }
 def maptest(x:Int):Int={
   x+pre.get("a")
 }
 def test1(s:String,b:String)={
   "hello"
 }
}