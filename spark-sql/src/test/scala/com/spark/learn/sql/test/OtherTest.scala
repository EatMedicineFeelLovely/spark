package com.spark.learn.sql.test

import com.spark.code.udt.{HyperLogLog, RegisterSet}
import org.apache.spark.sql.types.udt.HyperLogLog2
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class OtherTest extends FunSuite with BeforeAndAfterAll {
  def log2m(rsd: Double): Int =
    (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2)).toInt

  /**
   *
   */
  test("HyperLogLog2") {
//    val a = new HyperLogLog2(log2m(0.05))
//    a.offer("d1")
//    a.offer("d2")
//    a.offer("d3")
//    a.offer("d1")
//
//    val uv1 = a.cardinality()
//    println(uv1) //3
//
//    val b = new HyperLogLog2(log2m(0.05))
//    b.offer("d4")
//    b.offer("d2")
//    b.offer("d5")
//    b.offer("d1")
//    val uv2 = b.cardinality()
//    println(uv2) // 4
//
//    val b_back = HyperLogLog.Builder.build(b.getBytes)
//    println(b_back.cardinality()) // 4
//    a.addAll(b_back) // 5
//    println(a.cardinality())
  }
}
