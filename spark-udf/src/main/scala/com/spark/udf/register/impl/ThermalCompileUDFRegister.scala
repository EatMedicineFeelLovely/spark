package com.spark.udf.register.impl

import org.apache.spark.sql.SparkSession

/**
  *  热编译
  */
class ThermalCompileUDFRegister(spark: SparkSession, map: Map[String, String])
    extends UDFRegister(spark) {
  override def register(): Unit = {}

}
