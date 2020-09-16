package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{
  TableJoinHbaseInfoOperation,
  TableQueryInfo
}
import com.spark.sql.engine.common.UserDefinedFunction2
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class JoinHbaseImplExecutionImpl(
    df: Dataset[Row],
    queryInfo: TableQueryInfo,
    udfManager: mutable.HashMap[String, UserDefinedFunction2])
    extends AbstractExecution {

  def exec(): DataFrame = {
    // 原始DF的schame
    val schame = getSchameIndexMap(df)
    val newSchemeMap = getNewSchame(queryInfo.columnsInfo, schame)(udfManager)
    // rowkey在df中的index位置
    val indexHbaseRowkey = schame(
      queryInfo.joinInfo.joinKey.head.right.colName.toString)._1

    df.mapPartitions(itor => {
      val zk = queryInfo.confInfo("ZK")
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
              newSchemeMap.map { info =>
                execUdf(info, row, result)
              }
            Row.fromSeq(newR)
          }
        }
        .toIterator
    })(RowEncoder(StructType(newSchemeMap.map(_.colSchameInfo.structType))))
  }
}
