package com.spark.sql.engine.common

object SQLEnumeration extends Enumeration {
  type SQLEnumeration = Value
  val BASIC_COL = Value(1, "basic_col")
  val HBASE_COL = Value(4, "hbase_col")
  val CONSTANTPARAM_COL = Value(2, "CONSTANTPARAM_COL")
  val UDF_COL = Value(3, "udfCol")
}
