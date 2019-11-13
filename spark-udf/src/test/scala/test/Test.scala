package test

import com.spark.udf.core.{
  MethodInfo,
  MethodToScalaFunction,
  UDFClassLoaderManager
}
import com.spark.udf.register.{
  HiveUDFRegister,
  ThermalCompileUDFRegister,
  UrlJarUDFRegister
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

import scala.util.Try

object Test {
  var udfLoader: UDFClassLoaderManager = null
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local").appName("ss").getOrCreate()
    import spark.implicits._
    // testLocalFunc
    // testRDDFunc(spark)
    testDFUDF(spark)
//    spark.sparkContext
//      .parallelize(Array("20190101", "20190101"))
//      .toDF("a")
//      .createOrReplaceTempView("test")
//    spark.sql(s"select getLastWeek(a) as A from test").show

  }

  /**
    * 测试本地得方法调用，也就是平常使用得类反射
    */
  def testLocalFunc(): Unit = {
    val udfLoader = UDFClassLoaderManager()
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
      "file:/D:\\workspace\\mobutilgit\\mobutils\\dist\\lib\\mobutils-core-v0.1.4.jar")
    udfLoader
      .registerClass(new UrlJarUDFRegister(jars, func),
                     new ThermalCompileUDFRegister(codes))
      .foreach(println)
//    val mth = udfLoader.getUDF(s"com.mob.mobutils.util.DateUtils.getLastWeek")
//    val r = mth.call[String]("20190101") // 作为正常func使用
    val mth = udfLoader.getUDF(s"defualt.currentTime")
    val r = mth.call[String]()
    println(r)
  }

  /**
    * 测试在rdd算子里面做func操作
    * @param spark
    */
  def testRDDFunc(spark: SparkSession): Unit = {
    val func = Array(("com.mob.mobutils.util.DateUtils", "*"))
    val jars = Array(
      "file:/D:\\workspace\\mobutilgit\\mobutils\\dist\\lib\\mobutils-core-v0.1.4.jar")
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
  def testDFUDF(spark: SparkSession): Unit = {

    val udfLoader = UDFClassLoaderManager()

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
      "file:/D:\\workspace\\mobutilgit\\mobutils\\dist\\lib\\mobutils-core-v0.1.4.jar")


    udfLoader
      .registerUDF(spark, new UrlJarUDFRegister(jars, func),new ThermalCompileUDFRegister(codes))
      // .foreach(println)



    import spark.implicits._

    spark.sparkContext
      .parallelize(Array("20191201", "20191101"))
      .toDF("a")
      .createOrReplaceTempView("test")

    spark.sql(s"select getLastWeek(a) as A from test").show
  }

  /**
    *
    */
  def testThermalCompileUDF(): Unit = {}
}
