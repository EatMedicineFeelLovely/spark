package com.antrl4.visit.operation.impl

/**
  * 查询hbase的
 *
 * @param tableName
  * @param key
  * @param familyColumns
  */
case class HbaseSearchInfoOperation(
    tableName: String,
    key: String,
    familyColumns: Seq[HbaseFamilyColumnsInfoOperation])
    extends AnyRef {}

case class HbaseFamilyColumnsInfoOperation(
    familyName: String,
    columns: Seq[HbaseColumnsInfoOperation])
    extends AnyRef {}

case class HbaseColumnsInfoOperation(colName: String, colType: String)
    extends AnyRef {}
// ----------------------------------------------------------------------

//case class HbaseJoinInfoOperation(newTablename: String,
//                                  columnsInfo: Seq[ColumnsWithUdfInfoOperation],
//                                  tablename: String,
//                                  hbasetable: String,
//                                  joinkey: String,
//                                  zk: String)
// -------------------------------------------------------------
