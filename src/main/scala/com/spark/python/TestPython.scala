package com.spark.python

object TestPython {
  def main(args: Array[String]): Unit = {
    val p=Runtime.getRuntime().exec("./test.py")
    p.waitFor()
    println(p.exitValue())
  }
}