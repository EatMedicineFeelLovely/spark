package com.antrl4.visit.operation.impl

case class HbaseSearchInfoOperation(
                                     tableName: String,
                                     key: String,
                                     familyColumns: Seq[HbaseFamilyColumnsInfoOperation])
  extends AbstractVisitOperation {}

case class HbaseFamilyColumnsInfoOperation(
                                            familyName: String,
                                            columns: Seq[HbaseColumnsInfoOperation])
  extends AbstractVisitOperation {}

case class HbaseColumnsInfoOperation(colName: String, colType: String)
  extends AbstractVisitOperation {}

case class HbaseJoinInfoOperation(cols: Seq[HbaseJoincolumn],
                                  tablename: String,
                                  hbasetable: String,
                                  joinkey: String)
  extends AbstractVisitOperation


case class HbaseJoincolumn(family: String, colname: String)
  extends AbstractVisitOperation
