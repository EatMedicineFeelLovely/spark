//package com.spark.learn
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
//object TestSparkSql {
//
//  /**
//    *
//    * @param args
//    */
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      //.appName("test")
//      //.master("local")
//      .getOrCreate()
//    import spark.implicits._
//    spark.sparkContext.setLogLevel("ERROR")
//    val df2 = spark.sparkContext
//      .textFile("/user/dmp/test/100w/*")
//      .map(x => kv(x.toInt, x.toInt))
//      .toDF()
//    df2.orderBy("")
//    import org.apache.spark.sql.expressions.Window
//    case class kv(k: Int, v: Int)
//    val df = spark.sparkContext
//      .parallelize(1 to 100, 15)
//      .map(x => kv(x / 5, x))
//      .toDF //.cache
//    df.rdd
//      .mapPartitionsWithIndex {
//        case (i, itor) => List((i, itor.size)).toIterator
//      }
//      .collect()
//      .foreach(println)
//
//    df2
//      .withColumn("tt", row_number().over(Window.partitionBy("k").orderBy("v")))
//      .show()
//    Thread.sleep(10000)
////    spark.sql(s"""
////                 |SELECT k,v,test_udf(k,v) as k_v_sum
////                 |FROM testTable
////                 |WHERE k > ${10}
////      """.stripMargin).show
//
////    spark.sql(s"""
////                 |SELECT k,self_max(v) as max_v
////                 |FROM testTable
////                 |WHERE k > ${10}
////                 |group by k
////      """.stripMargin).show
//  }
//
//  /**
//    * 自定义uid
//    */
//  val testUdf = (x: Int, y: Int) => x + y
//
//  val testUdf2 = new Function[Object, Object] {
//    override def apply(v1: Object): AnyRef = ""
//  }
//
//}
