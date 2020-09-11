package com.spark.sql.engine

import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory._
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{
  TableJoinHbaseInfoOperation,
  TableSelectInfoOperation
}
import com.spark.sql.engine.common.SQLEnumeration._
import com.spark.sql.execution.impl.{
  JoinHbaseImplExecutionImpl,
  TableSelectExecutionImpl
}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
class SparksqlEngine(override val spark: SparkSession)
    extends AbstractSqlEngine(spark) {
  import spark.implicits._

  /**
    *
    * @param sqltext
    * @return
    */
  def sql(sqltext: String): DataFrame = {
    try {
      spark.sql(sqltext)
    } catch {
      case e: Throwable =>
        // e.printStackTrace()
        println("这是一个自定义的sql语法，使用 mob sql 引擎执行")
        mobSql(sqltext)
    }
  }


  /**
   * mob sql 执行引擎
   * @param sqltext
   * @return
   */
  def mobSql(sqltext: String): DataFrame ={
    visit(sqltext) match {
      case b: CheckpointVisitOperation => {
        spark.table(b.table).write.csv(b.location)
        spark.table(b.table)
      }
      case queryInfo: TableQueryInfo => {
        val df = spark.table(queryInfo.sourceTableInfo.tableName)
        val resultDf = queryInfo.sourceTableInfo.tableType match {
          case HBASE_TABLE => null
          case BASIC_TABLE =>
            // join逻辑
            if (queryInfo.joinInfo != null) {
              if (queryInfo.joinInfo.rightTb.tableType == HBASE_TABLE) {
                JoinHbaseImplExecutionImpl(
                  spark.table(queryInfo.sourceTableInfo.tableName),
                  queryInfo,
                  udfManager).exec
                // join hbase
              } else { // 普通的join
                null
              }
            } else {
              // 普通的查询
              TableSelectExecutionImpl(df, queryInfo, udfManager).exec()
            }
          case HIVE_TABLE => null


        }

        resultDf.createOrReplaceTempView(queryInfo.targetTableInfo.tableName)
        spark.table(queryInfo.targetTableInfo.tableName)

      }
      case err: Any =>
        println("未匹配对应的 VisitOperation ")
        println(err)
        null
    }
  }

}

object SparksqlEngine {
  var engine: SparksqlEngine = null
  def apply(spark: SparkSession): SparksqlEngine = {
    if (engine == null)
      engine = new SparksqlEngine(spark)
    engine
  }
}
