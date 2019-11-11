package com.spark.udf.register.impl

import org.apache.spark.sql.SparkSession

class HiveUDFRegister(spark: SparkSession) extends UDFRegister(spark) {}
