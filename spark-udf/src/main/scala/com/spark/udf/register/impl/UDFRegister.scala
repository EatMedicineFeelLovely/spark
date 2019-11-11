package com.spark.udf.register.impl

import org.apache.spark.sql.SparkSession

class UDFRegister(spark: SparkSession) extends UDFRegisterTrait {

  override def register(): Unit = ???

}
