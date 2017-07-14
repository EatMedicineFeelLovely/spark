package com

import org.apache.spark.rdd.RDD
import com.test.SparkWithLocalTest.TraitTest
import com.test.SparkWithLocalTest.classTest
package object test extends TraitTest{
def fun1(x:(Int,Int))={
    fun2(x)
  }
  implicit def fun4(rdd:RDD[(Int,Int)])=new classTest(rdd)
}