package com.test

object ReflectScala {
  def main(args: Array[String]): Unit = {
    var a=Class.forName("com.test.ReflectScala")
    //因为该方法是一个静态的方法，所以这个地方的invoke只要填null就可以了。但是如果不是一个静态方法，就需要一个实例
    //a.getMethod("test").invoke(a.newInstance())
    a.getMethod("test",classOf[String]).invoke(null,"hello world")
    
  }
  def test(s:String){
    println(s)
  }
}