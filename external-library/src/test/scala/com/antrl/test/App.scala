package com.antrl.test

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.operation.impl.{
  AbstractVisitOperation,
  CheckpointVisitOperation,
  HbaseJoinInfoOperation,
  HbaseSearchInfoOperation,
  HelloWordVisitOperation
}
import com.antrl4.visit.parser.impl.CustomSqlParserVisitorImpl
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.antlr.v4.runtime.{
  CharStreams,
  CodePointCharStream,
  CommonTokenStream
}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

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
        //        df.map(r => {
        //          val c =
        //          d.cols.map(x => {
        //            if(x.family == null || x.family.equals("null") || x.family.isEmpty){
        //              r.get(schame(x.colname)._1)
        //            } else {
        //              "hbaseV"
        //            }
        //          })
        //        Row.fromSeq(c)
        //        })(RowEncoder(newSchema))
        //          .show
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
  def joinHbase(df: Dataset[Row], d: HbaseJoinInfoOperation): Unit = {
    // 原始DF的schame
    val schame = df.schema
      .map(x => (x.name, x))
      .zip(0 to (df.schema.size - 1))
      .map(x => (x._1._1, (x._2, x._1._2)))
      .toMap // schamename -> (index, struct)
    // sql之后的 schame
    var newSchema = new StructType()
    d.cols.foreach(x => {
      if (x.family == null || x.family.equals("null") || x.family.isEmpty) {
        newSchema = newSchema.add(schame(x.colname)._2)
      } else { // hbase的数据统一string类型
        newSchema = newSchema.add(x.family + "." + x.colname, "string")
      }
    })
    val indexHbaseRowkey = schame(d.joinkey)._1
    df.mapPartitions(itor => {
      val list = itor.toList
      // hbase conn
      val gets = list.map { x =>
        x.get(indexHbaseRowkey).toString
      }
      // hbase get

      gets.zip(list).foreach{
        case(res, row) =>

      }







      itor
    })(RowEncoder(newSchema))

  }
}
