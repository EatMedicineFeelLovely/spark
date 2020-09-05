package com.spark.sql.engine

import com.antrl4.visit.operation.impl.{CheckpointVisitOperation, HbaseSearchInfoOperation, TableInfoVisitOperationFactory}
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{TableJoinHbaseInfoOperation, TableSelectInfoOperation}
import com.spark.sql.execution.impl.{JoinHbaseImplExecutionImpl, TableSelectExecutionImpl}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
class SparksqlEngine(override val spark: SparkSession) extends AbstractSqlEngine(spark) {
  import spark.implicits._
  /**
    * 执行sql，
    * @param sqltext
    * @return
    */
  def sql(sqltext: String): DataFrame = {
    try {
      visit(sqltext) match {
        case b: CheckpointVisitOperation => {
          spark.table(b.table).write.csv(b.location)
          spark.table(b.table)
        }
        case c: HbaseSearchInfoOperation => {
          Seq("test").toDF
        }
        case d: TableJoinHbaseInfoOperation => {
          if (d.resultTablename != null && d.resultTablename.nonEmpty) {
            JoinHbaseImplExecutionImpl(spark.table(d.joinTablename), d, udfManager)
              .exec()
              .createOrReplaceTempView(d.resultTablename)
            spark.table(d.resultTablename)
          } else {
            JoinHbaseImplExecutionImpl(spark.table(d.joinTablename), d, udfManager).exec
          }
        }
        case e: TableSelectInfoOperation => {
          println(e)
          val df = spark.table(e.resultTablename)
          // 原始DF的schame
          TableSelectExecutionImpl(df, e, udfManager).exec()
        }
        case err: Any =>
          println("未匹配对应的 VisitOperation ")
          println(err)
          null
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        println(e.toString)
        println("这不是一个自定义的sql语法，使用 spark sql 引擎执行")
        spark.sql(sqltext)
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
