import com.spark.code.udt.{HyperLogLog, RegisterSet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.udt.{HyperLogLog2}

object UDTTest {
  def log2m(rsd: Double): Int =
    (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2)).toInt
  def main(args: Array[String]): Unit = {
    countdistinct
  }

  case class Data(key: String, uid: String)

  /**
    * 使用自定义UDT，用HyperLog算法来粗略计算UV。主要用于流式计算中计算UV的时候使用
    */
  def countdistinct(): Unit = {
    val d = Array(Data("a", "d1"),
                  Data("a", "d2"),
                  Data("a", "d1"),
                  Data("b", "d1"),
                  Data("b", "d1"),
                  Data("b", "d1"))
    val spark =
      SparkSession.builder().master("local").appName("ll").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    spark
      .createDataFrame(spark.sparkContext.parallelize(d))
      .createOrReplaceTempView("test")
    val df1 = spark
      .table("test")
      .rdd
      .groupBy(_.getString(0))
      .mapPartitions { itor => // itor(string,list)
        itor.map {
          case (uid, list) =>
            val hyperLogLog2 =
              new HyperLogLog2(log2m(0.05), new RegisterSet(1 << log2m(0.05)))
            list.foreach(r => hyperLogLog2.offer(r.getString(1)))
            (uid, list.size, hyperLogLog2)
        }
      }

    /**
      * +---+---+---+
      * |key|pv |uv |
      * +---+---+---+
      * |b  |3  |1  |
      * |a  |3  |2  |
      * +---+---+---+
      */
    df1.foreach(println)

    /**
      * +---+---+---+
      * |key|pv |uv |
      * +---+---+---+
      * |b  |6  |1  |
      * |a  |6  |2  |
      * +---+---+---+
      * 在流式计算中，可以定义 UDFA 来计算UV，将HyperLogLog2转Array[Byte]存入redis或者hbase每个批次可以提取出来做addAll
      */
    df1
      .union(df1)
      .groupBy(_._1)
      .mapPartitions { itor =>
        itor.map {
          case (uid, l) =>
            val list = l.toList
            var (_, pv, uv) = list.head
            for (i <- 1 to list.size - 1) {
              val (_, p, u) = list(i)
              pv += p
              uv.addAll(u)
            }
            (uid, pv, uv.cardinality())
        }
      }
      .toDF("key", "pv", "uv")
      .show(false)

    /**
      * +---+---+---+
      * |key| pv| uv|
      * +---+---+---+
      * |  b|  3|  1|
      * |  a|  3|  2|
      * +---+---+---+
      */
    spark
      .sql(
        s"select key,count(uid) pv,count(distinct(uid)) as uv from test group by key")
      .show
  }
}
