package com.spark.sql.engine.common

object SQLEnumeration extends Enumeration {
  type SQLEnumeration = Value
  val BASIC_COL = Value(1, "BASIC_COL")
  val HBASE_COL = Value(4, "HBASE_COL")
  val CONSTANTPARAM_COL = Value(2, "CONSTANTPARAM_COL")
  val UDF_COL = Value(3, "UDF_COL")


  val HBASE_TABLE = Value(5, "HBASE_TABLE")
  val BASIC_TABLE = Value(6, "BASIC_TABLE")
  val HIVE_TABLE = Value(7, "HIVE_TABLE")
}
