package com.spark.learn

import com.spark.learn.TestSparkSql.testUdf
import org.apache.spark.sql.SparkSession

package object udf {
  /**
    * 注册需要的udf
    * @param spark
    */
  def registerUDF(spark: SparkSession): Unit = {
    spark.udf.register("test_udf", testUdf)
    spark.udf.register("self_max", new SparkSqlMaxUDFA)

  }
}
