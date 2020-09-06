package com.antrl4.visit.operation.impl

import com.spark.sql.engine.common.UserDefinedFunction2
import org.apache.spark.sql.types.StructField

object ColumnsVisitOperationFactory {


  case class ColumnsInfo(
                          colName: ColumnsNameInfo,
                          colSchameInfo: ColumnsSchameInfo,
                          udfInfo: UdfInfo
                          )
  case class ColumnsSchameInfo(colType: String, rowIndex: Int, structType: StructField)
  case class ColumnsNameInfo(colName: String, family: String = null){
    override def toString: String = {
      if(family ==null) {colName} else family + "." +colName

    }
  }
  case class UdfInfo(udfName: String, paramCols: Seq[ColumnsInfo], userDefinedFunc: UserDefinedFunction2)



//  case class ColumnsWithUdfInfoOperation(udfName: String,
//                                         paramCols: Seq[ColumnsInfoWithRowIndexOperation],
//                                         newColsName: ColumnsInfoOperation,
//                                         colName: String,
//                                         colType: String,
//                                         index: Int,
//                                         family: String= null)
//
//
//      extends AnyRef {}
//
//  case class ColumnsInfoOperation(colName: String, colType: String, family: String= null)
//
//  case class ColumnsInfoWithRowIndexOperation(colName: String, colType: String, index: Int, family: String= null)

}
