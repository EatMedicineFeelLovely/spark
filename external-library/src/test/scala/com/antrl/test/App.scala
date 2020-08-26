package com.antrl.test

import java.util

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.operation.impl.{AbstractVisitOperation, CheckpointVisitOperation, HbaseJoinInfoOperation, HbaseSearchInfoOperation, HelloWordVisitOperation}
import com.antrl4.visit.parser.impl.CustomSqlParserVisitorImpl
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{types, Dataset, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * @author ${user.name}
  */
class App extends SparkFunSuite with ParamFunSuite {

  import spark.implicits._

  /**
    *
    */
  test("ckp test") {
    val inputStream2 = CharStreams.fromString(
      "checkpoint ab.`table` into 'hdfs:///ssxsxs/ssxxs'")
    visit(inputStream2) match {
      case a: HelloWordVisitOperation  => println(a)
      case b: CheckpointVisitOperation => println(b)
      case c: HbaseSearchInfoOperation => println(c)
      case d: HbaseJoinInfoOperation   => println(d)
      case _                           => println("xxx")
    }
  }

  /**
    *
    */
  test("select hbase") {
    val inputStream3 = CharStreams.fromString(
      "select info(name1 string , name2 string),info3(name3 string , name4 string) FROM hbasetable where key='abc'")
    visit(inputStream3) match {
      case a: HelloWordVisitOperation  => println(a)
      case b: CheckpointVisitOperation => println(b)
      case c: HbaseSearchInfoOperation => println(c)
      case d: HbaseJoinInfoOperation   => println(d)
      case _                           => println("xxx")
    }
  }

  /**
    *
    */
  test("hbase join") {
    val inputStream4 = CharStreams.fromString(
      "select word,count,info.ac FROM lefttable JOIN default:hbasetable" +
        " ON ROWKEY = word" +
        " CONF ZK = 'localhost:2181'")
    visit(inputStream4) match {
      case a: HelloWordVisitOperation  => println(a)
      case b: CheckpointVisitOperation => println(b)
      case c: HbaseSearchInfoOperation => println(c)
      case d: HbaseJoinInfoOperation => {
        val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
          RowEncoder(schame))
        val newDf = joinHbase(df, d)
        newDf.printSchema()
        newDf.show
      }
      case _ => println("xxx")
    }

  }

  def visit(inputStream: CodePointCharStream): AbstractVisitOperation = {
    val lexer = new CustomSqlParserLexer(inputStream)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CustomSqlParserParser(tokenStream)
    val state = parser.customcal()
    val visitor = new CustomSqlParserVisitorImpl()
    visitor.visit(state)
  }

  val schame = new StructType()
    .add("word", "string")
    .add("count", "int")

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
                      new String(result.getValue(family.getBytes(), colname.getBytes()))
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
