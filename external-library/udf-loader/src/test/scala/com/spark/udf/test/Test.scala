package com.spark.udf.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import com.spark.udf.core.UDFClassLoaderManager
import com.spark.udf.register.{DynamicCompileUDFRegister, UrlJarUDFRegister}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.rdd.MapPartitionsRDD
// 支持 scala 2.12.8
class Test extends SparkFunSuite with ParamFunSuite {
  import spark.implicits._
  spark.sparkContext
    .parallelize(Array("20191201", "20191101"))
    .toDF("a")
    .createOrReplaceTempView("test")

  /**
    *
    *
    */
  test("url java jar load") {
    val udfLoader = UDFClassLoaderManager()
    val func = Array(("com.spark.udf.bean.UdfLoaderJavaTest", "*"))
    val jars = Array(
      "file://Users/eminem/workspace/git_pro/spark-learn/external-library/udf-loader/target/udf-loader-2.4.0.jar")
    udfLoader.registerUDF(spark, new UrlJarUDFRegister(jars, func))
    udfLoader.udfMethodInfos.foreach(println)
    val mth =
      udfLoader.getUDF("com.spark.udf.bean.UdfLoaderJavaTest", s"getCurrentDay")
    val r = mth.call[String]()
    println(r)
    spark.sql(s"select getCurrentDay() as A from test").show

  }

  /**
    * 测试本地得方法调用，也就是平常使用得类反射
    */
  test("test scala code Func") {
    val udfLoader = UDFClassLoaderManager()
    val format = "yyyy-MM-dd HH:mm"
    //className , codes
    val codes = Array(
      ("",
       s"""import java.text.SimpleDateFormat
           import java.util.Calendar
           import org.apache.commons.lang3.time.DateFormatUtils
           def currentTime(): String = {
                  DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm")
           }""".stripMargin))

    udfLoader
      .registerUDF(spark, new DynamicCompileUDFRegister(codes)) // 多种注册方式
    val mth = udfLoader.getUDF(s"currentTime")

    val r = mth.call[String]() // 作为正常func使用
    println(r)
    spark.sql(s"select currentTime() as A from test").show
  }

  /**
    * 测试在rdd算子里面做func操作
    */
  test("testRDDFunc") {
    val func = Array(("com.spark.udf.bean.UdfLoaderJavaTest", "*"))
    val jars = Array(
      "file://Users/eminem/workspace/git_pro/spark-learn/external-library/udf-loader/target/udf-loader-2.4.0.jar")
    spark.sparkContext
      .parallelize(Array("20191201", "20191101"))
      .repartition(1)
      .mapPartitions(itor => {
        val udfLoader = UDFClassLoaderManager()
        udfLoader.registerUDF(null, new UrlJarUDFRegister(jars, func))
        val mth =
          udfLoader.getUDF("com.spark.udf.bean.UdfLoaderJavaTest", s"getCurrentDay")
        itor.map(x => {
          mth.call[String]() // 作为正常func使用
        })
      })
      .foreach(println)
  }

  /**
    * testRDDFunc: 分区里面只初始化一次。应用于流式计算，防止重复加载

    * @return
    */
//  def initudfLoader(jars: Array[String],
//                    func: Array[(String, String)]): UDFClassLoaderManager = {
//    if (udfLoader == null) {
//      udfLoader = UDFClassLoaderManager()
//      udfLoader.registerUDF(null, new UrlJarUDFRegister(jars, func))
//    }
//    udfLoader
//  }

  /**
    * udf
    */
  test("testDFUDF") {
    val udfLoader = UDFClassLoaderManager()

    val codes = Array(
      ("",
       s"""import java.text.SimpleDateFormat
           import java.util.Calendar
           import org.apache.commons.lang3.time.DateFormatUtils
              def currentTime(): String = {
           DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
           }""".stripMargin))

    val func = Array(("com.spark.udf.bean.UdfLoaderJavaTest", "*"))
    val jars = Array(
      "file://Users/eminem/workspace/git_pro/spark-learn/external-library/udf-loader/target/udf-loader-2.4.0.jar")

    udfLoader
      .registerUDF(spark,
                   new UrlJarUDFRegister(jars, func),
                   new DynamicCompileUDFRegister(codes))
    udfLoader.udfClassInfos.foreach(println)
    udfLoader.udfMethodInfos.foreach(println)

    spark.sparkContext
      .parallelize(Array("20191201", "20191101"))
      .toDF("a")
      .createOrReplaceTempView("test")

    spark.sql(s"select currentTime() as A, getCurrentDay() as C from test").show
  }
}
