package com.spark.udf.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import com.spark.udf.core.UDFClassLoaderManager
import com.spark.udf.register.{DynamicCompileUDFRegister, UrlJarUDFRegister}
import org.apache.spark.sql.SparkSession
// 支持 scala 2.12.8
class Test extends SparkFunSuite with ParamFunSuite {
  var udfLoader: UDFClassLoaderManager = null

  test("url jar load"){

  }

  /**
    * 测试本地得方法调用，也就是平常使用得类反射
    */
  test("testLocalFunc") {
    val udfLoader = UDFClassLoaderManager()
    val codes = Array(
      ("",
       s"""import java.text.SimpleDateFormat
           import java.util.Calendar
           import org.apache.commons.lang3.time.DateFormatUtils
              def currentTime(): String = {
           DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
           }""".stripMargin))

    // val func = Array(("com.mob.mobutils.util.DateUtils", "*"))
   // val jars = Array("file://data/dd/dd/dd/mobutils-core-v0.1.5.jar")
    udfLoader
      .registerUDF(spark, new DynamicCompileUDFRegister(codes))  // 多种注册方式
      // .getMethodInfo("defualy.currentTime")
//    val mth = udfLoader.getUDF(s"com.mob.mobutils.util.DateUtils.getLastWeek")
//    val r = mth.call[String]("20190101") // 作为正常func使用
    val mth = udfLoader.getUDF(s"currentTime")
    val r = mth.call[String]()
    println(r)
  }

  /**
    * 测试在rdd算子里面做func操作
    */
  test("testRDDFunc"){
    val func = Array(("com.mob.mobutils.util.DateUtils", "*"))
    val jars = Array(
      "file:/D:\\workspace\\mobutilgit\\mobutils\\dist\\lib\\mobutils-core-v0.1.5.jar")
    spark.sparkContext
      .parallelize(Array("20191201", "20191101"))
      .mapPartitions(itor => {
        val udfLoader = initudfLoader(jars, func)
        val mth =
          udfLoader.getUDF(s"com.mob.mobutils.util.DateUtils.getLastWeek")
        itor.map(x => {
          mth.call[String](x) // 作为正常func使用
        })
      })
      .foreach(println)
  }

  /**
    * testRDDFunc: 分区里面只初始化一次。应用于流式计算，防止重复加载
    * @param jars
    * @param func
    * @return
    */
  def initudfLoader(jars: Array[String],
                    func: Array[(String, String)]): UDFClassLoaderManager = {
    if (udfLoader == null) {
      udfLoader = UDFClassLoaderManager()
      udfLoader.registerClass(new UrlJarUDFRegister(jars, func))
    }
    udfLoader
  }

  /**
    * udf
    */
  test("testDFUDF"){

    val codes = Array(
      ("",
       s"""import java.text.SimpleDateFormat
           import java.util.Calendar
           import org.apache.commons.lang3.time.DateFormatUtils
              def currentTime(): String = {
           DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
           }""".stripMargin))

    val func = Array(("com.mob.mobutils.util.DateUtils", "*"))
    val jars = Array(
      "file:/D:\\workspace\\mobutilgit\\mobutils\\dist\\lib\\mobutils-core-v0.1.5.jar")

    UDFClassLoaderManager()
      .registerUDF(spark,
                   new UrlJarUDFRegister(jars, func),
                   new DynamicCompileUDFRegister(codes))
    // .foreach(println)

    import spark.implicits._

    spark.sparkContext
      .parallelize(Array("20191201", "20191101"))
      .toDF("a")
      .createOrReplaceTempView("test")

    spark.sql(s"select currentTime() as A from test").show
  }
}
