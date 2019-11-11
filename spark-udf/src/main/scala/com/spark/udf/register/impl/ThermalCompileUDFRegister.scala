package com.spark.udf.register.impl

import org.apache.spark.sql.SparkSession

/**
  *  热编译
  */
class ThermalCompileUDFRegister(map: Map[String, String])
    extends UDFRegisterTrait {
  override def register(spark: SparkSession): Unit = {}

}
