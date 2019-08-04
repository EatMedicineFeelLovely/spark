package com.spark.session

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSessionTest {
  val colums=Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      //.master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val dataList: Array[(Double, String, Double, Double, String, Double, Double, Double, Double)] = Array(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5))
      val rdd=spark.sparkContext.parallelize(dataList).map{x=>println("xxx");x}
      val dataSet=rdd.toDF(colums:_*)
      dataSet.selectExpr("count(distinct age) as uv").show
    val r1=spark.sparkContext.parallelize(Array((1,2),(1,3)))
    val r2=spark.sparkContext.parallelize(Array((1,4),(1,5),(2,1)))
    r1.leftOuterJoin(r2).foreach(println)
    
  }
  def listToDF(){
  }
  
}