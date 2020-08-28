package com.spark.sql.engine

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.operation.impl.{
  CheckpointVisitOperation,
  HbaseJoinInfoOperation,
  HbaseSearchInfoOperation
}
import com.antrl4.visit.parser.impl.{
  CustomSqlParserVisitorImpl,
  ExtErrorListener
}
import org.antlr.v4.runtime.{
  CharStreams,
  CodePointCharStream,
  CommonTokenStream
}
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class SparksqlEngine(spark: SparkSession) {
  import spark.implicits._
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
        case d: HbaseJoinInfoOperation => {
          joinHbase(spark.table(d.tablename), d)
        }

        case _ => null
      }
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        println("这不是一个自定义的sql语法，使用 spark sql 引擎执行")
        spark.sql(sqltext)
    }

  }

  /**
    *
    * @param sqltext
    * @return
    */
  def visit(sqltext: String): AnyRef = {
    val inputStream = CharStreams.fromString(sqltext)
    val lexer = new CustomSqlParserLexer(inputStream)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CustomSqlParserParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(new ExtErrorListener) // 用来抛异常的
    val state = parser.customcal()
    val visitor = new CustomSqlParserVisitorImpl()
    visitor.visit(state)
  }

  /**
    *
    * @param df
    * @param d
    */
  def joinHbase(df: Dataset[Row], d: HbaseJoinInfoOperation): Dataset[Row] = {
    // 原始DF的schame
    val schame = df.schema
      .map(x => (x.name, x))
      .zip(0 to (df.schema.size - 1))
      .map(x => (x._1._1, (x._2, x._1._2)))
      .toMap // schamename -> (index, struct)
    // sql之后的 schame的map信息,index位置等
    val newSchameMap = new ArrayBuffer[((String, String), (Int, StructField))]()
    d.cols.foreach(x => {
      if (x.family == null || x.family.equals("null") || x.family.isEmpty) {
        newSchameMap.+=(((x.family, x.colname), schame(x.colname)))
      } else { // hbase的数据统一string类型
        newSchameMap.+=(
          ((x.family, x.colname),
           (-1, StructField(x.family + "." + x.colname, StringType))))
      }
    })
    // 新df的schame
    val newSchame = StructType(newSchameMap.map(_._2._2))
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
              newSchameMap.map {
                case ((family, colname), (index, tp)) =>
                  if (index < 0) {
                    // hbase字段
                    if (result == null) {
                      "null"
                    } else {
                      new String(
                        result.getValue(family.getBytes(), colname.getBytes()))
                    }
                  } else {
                    tp.dataType.typeName
                    row.get(index)
                  }
              }
            Row.fromSeq(newR)
          }
        }
        .toIterator
    })(RowEncoder(newSchame))

  }
}
