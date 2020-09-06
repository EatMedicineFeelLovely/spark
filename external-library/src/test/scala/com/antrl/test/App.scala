package com.antrl.test
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import com.spark.sql.engine.SparksqlEngine
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.{Row}

/**
  * @author ${user.name}
  */
class App extends SparkFunSuite with ParamFunSuite {

  import spark.implicits._
  val sparkEngine = SparksqlEngine(spark)

  /**
    * 测试spark的sql功能
    */
  test("spark sql") {
    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")
    sparkEngine.sql(
      "CREATE OR REPLACE TEMPORARY VIEW fence_flow AS" +
        " select * from lefttable")
    sparkEngine.spark.table("fence_flow").show
  }

  /**
    *
    */
  test("ckp test") {
    sparkEngine.sql("checkpoints ab.`table` into 'hdfs:///ssxsxs/ssxxs'")
  }

  /**
    *
    */
  test("select hbase") {
    sparkEngine.sql(
      "select info(name1 string , name2 string),info3(name3 string , name4 string) FROM hbasetable where key='abc'")
  }

  /**
    *
    */
  test("hbase join") {
    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")
    sparkEngine.register("testudf", (a: String) => {a + ": UDF :RES"})

    sparkEngine
      .sql(
        s"""CREATE OR REPLACE TEMPORARY VIEW wordcountJoinHbaseTable AS
         | select word,count,testudf(info.ac) FROM lefttable JOIN default:hbasetable
         |ON ROWKEY = word
         |CONF ZK = 'localhost:2181'""".stripMargin
      )
      .show

    sparkEngine
      .sql(
        "select testudf(word) as tt,count,testudf(info.ac) FROM lefttable JOIN default:hbasetable" +
          " ON ROWKEY = word" +
          " CONF ZK = 'localhost:2181'")
      .show

  }

  test("udf test") {
    def f(a: Any) = { a + ": UDF"}
    def f2(a: Any, b: Any) = { a + " : " + b}

    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")
    sparkEngine.register("testudf", f _)
    sparkEngine.register("testudf2", f2 _)

    //spark.sql(s"select testudf(testudf(testudf(word))) as a, testudf2(word, count) as a2 from lefttable").show

    sparkEngine.sql(s"""select testudf(testudf(testudf(word))), testudf2(word, count)  from lefttable""").show()

  }


}
