package com.spark.learn.bloomf

import org.apache.spark.sql.SparkSession

object BloomFilterTest {
  /**
   * 与 hyper一样都是基于概率。bloom只判断是否存在，hyper判断有多少不重复
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("rr").getOrCreate()
    import spark.implicits._
    val df = spark.createDataset(spark.sparkContext.parallelize(1 to 100)).toDF("a")
      df.show

    val boolm = df.stat.bloomFilter($"a", 10000, 0.01)
    println(boolm.mightContain(1))
  }
}
