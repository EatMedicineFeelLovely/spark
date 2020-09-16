package com.antrl4.visit.operation.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import com.spark.sql.engine.common.SQLEnumeration.SQLEnumeration

object TableInfoVisitOperationFactory {

  case class TableSelectInfoOperation(var resultTablename: String,
                                      var columnsInfo: Seq[ColumnsInfo])
      extends AnyRef

  case class TableQueryInfo(targetTableInfo: TableInfo,
                            sourceTableInfo: TableInfo,
                            columnsInfo: Seq[ColumnsInfo],
                            joinInfo: JoinInfo,
                            whereInfo: WhereInfo,
                            confInfo: ConfInfo)
      extends AnyRef

  case class JoinInfo(rightTb: TableInfo, joinKey: Seq[WhereInfo]) {}

  case class WhereInfo(left: ColumnsInfo, right: ColumnsInfo, operate: String)

  case class ConfInfo(confMap: Map[String, String]) {
    def apply(key: String): String = confMap(key)
  }

  case class TableInfo(tableName: String, tableType: SQLEnumeration) {}
  case class TableJoinHbaseInfoOperation(resultTablename: String,
                                         columnsInfo: Seq[ColumnsInfo],
                                         joinTablename: String,
                                         hbaseTable: String,
                                         joinkey: String,
                                         zk: String)
      extends AnyRef

  case class DatasetCollectInfoOperation(paramName: String,
                                         tableName: String,
                                         actionName: String)
      extends AnyRef

  case class HelloWordVisitOperation(word: String) extends AnyRef
  case class CheckpointVisitOperation(table: String, location: String)
      extends AnyRef {}

  case class HbaseSearchInfoOperation(tableName: String,
                                      key: String,
                                      familyColumns: Seq[ColumnsInfo])
      extends AnyRef {}
}
