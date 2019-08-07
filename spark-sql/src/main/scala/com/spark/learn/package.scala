package com.spark

import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

package object learn {

  /**
    *两种方式可以选择
    */
  def getSchema() = {
//    StructType(
//      Array(
//        StructField("id", StringType),
//        StructField("age", IntegerType),
//        StructField("name", StringType)
//      ))
    new StructType()
      .add("id", "String")
      .add("age", "Int")
      .add("name", "String")
  }
}
