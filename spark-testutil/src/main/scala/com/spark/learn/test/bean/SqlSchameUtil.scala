package com.spark.learn.test.bean

import org.apache.spark.sql.types.StructType

object SqlSchameUtil {
  val WORD_COUNT_SCHAME = new StructType()
    .add("word", "string")
    .add("count", "int")

}
