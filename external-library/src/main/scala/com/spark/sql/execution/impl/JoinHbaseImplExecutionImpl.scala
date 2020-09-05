package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.TableJoinHbaseInfoOperation
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class JoinHbaseImplExecutionImpl(
    df: Dataset[Row],
    d: TableJoinHbaseInfoOperation,
    udfManager: mutable.HashMap[String, UserDefinedFunction])
    extends AbstractExecution {

  def exec(): DataFrame = {
    // 原始DF的schame
    val schame = getSchameIndexMap(df)
    val newSchemeMap = getNewSchame(d.columnsInfo, schame)(udfManager)
    // rowkey在df中的index位置
    val indexHbaseRowkey = schame(d.joinkey)._1
    df.mapPartitions(itor => {
      val zk = d.zk
      val list = itor.toList
      // hbase conn
      val rowkeyGets = list.map { x =>
        x.get(indexHbaseRowkey).toString
      }
      // hbase gets
      val result = Array.ofDim[Result](rowkeyGets.size)

      result
        .zip(list)
        .map {
          case (result, row) => {
            val newR =
              newSchemeMap.map {
                info =>
                  if (info.rowIndex < 0) { // udf 或者 hbase 字段
                    if (info.userDefinedFunc == null) { // hbase 字段
                      if (result == null) {
                        "null"
                      } else {
                        new String(
                          result.getValue(info.columnInfo.newColsName.family.getBytes(),
                                          info.columnInfo.newColsName.colName.getBytes()))
                      }
                    } else {
                      val udfParamsV = info.columnInfo.paramCols.map(x => {
                        if(x.family == null) {
                          row.get(x.index)
                        } else {
                         // new String(result.getValue(x.family.getBytes(), x.colName.getBytes()))
                          //  function
                         "no value"
                        }
                      })

                      val ff =
                        info.userDefinedFunc.inputTypes.size match {
                          case 1 =>
                            info.userDefinedFunc.f.asInstanceOf[
                              Function1[Any, info.userDefinedFunc.dataType.type]]
                        }
                      ff(udfParamsV.head)

                    }
                  } else {
                    row.get(info.rowIndex)
                  }
              }
            Row.fromSeq(newR)
          }
        }
        .toIterator
    })(RowEncoder(StructType(newSchemeMap.map(_.structType))))

  }
}
