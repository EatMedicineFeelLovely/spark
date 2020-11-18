package com.spark.learn.test.bean

import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType}

object SqlSchameUtil {
  val WORD_COUNT_SCHAME = new StructType()
    .add("word", "string")
    .add("count", "int")

  val JSON_SCHAME =
    StructType(
      Array(
        StructField("word", StringType, true),
        StructField("count", StringType, false)
      ))

  val ARR_MAP_STRUCT_SCHAME = new StructType()
    .add("arrtype", ArrayType(StringType))
    .add("maptype", MapType(StringType, StringType))
    .add("structtype",
         new StructType().add("t1", StringType).add("t2", StringType))

}
