package com.spark.learn

import org.apache.spark.{HashPartitioner, SparkContext}

/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]): Unit = {
    val sc = new SparkContext()

    val a = sc.parallelize(1 to 10,10)
    val b =a.zip(a).partitionBy(new HashPartitioner(10))
b.join(b)
    a.foreachPartition(x=>x.foreach(println))

    b.foreachPartition(x=>x.foreach(println))

  }

}
