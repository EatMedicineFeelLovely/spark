package com.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import com.spark.sql.CaseClass._
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
object SpaekSqlFunc {
  def selectFunc(spark: SparkSession) = {
    import spark.implicits._
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Array("1", "2", "3")).map { (Student(_)) }
    val df = rdd.toDS()
    //'是Symbol的一个方法，返回一个Symbol。在SQLImlicit里面有隐式方法：symbolToColumn
    df.select(
         when($"name" === "1", 1)
        .when('name === "2", 2)
        .otherwise(3)
        .as("name"))
      .show
    //df.select($"name"==="1" as "name").show
    //df.select('name).show
    //df.select($"name").show
  }
}