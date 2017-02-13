package com.test

import java.util.HashMap

object Test {
  def main(args: Array[String]): Unit = {
    //println(fun((1,1)))
    val a=new HashMap[String,String]
    a.put("a", "a")
    t1(a)
    println(a)
    t2(a)
    println(a)
    
    
  }
def t1(a:HashMap[String,String]){
  a.clear()
}
def t2(a:HashMap[String,String]){
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