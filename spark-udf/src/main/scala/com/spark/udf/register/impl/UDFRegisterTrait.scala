package com.spark.udf.register.impl

import org.apache.spark.sql.SparkSession

trait UDFRegisterTrait {
  def register(spark: SparkSession): Unit
}
