package com.spark.learn

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestWindowingfunction {
//   id， age，name
//  +---+---+---+
//  |001| 21| a|
//  |002| 23| b|
//  |003| 31| a|
//  |004| 27| b|
//  |005| 28| a|
//  |006| 25| b|
//  |007| 41| a|
//  |008| 33| b|
//  |009| 36| a|
//  |010| 42| b|
//  |011| 37| a|
//  |012| 41| c|
//  |013| 33| c|
//  |014| 31| d|
//  |015| 42| d|
//  |016| 57| d|
//  +---+---+---+
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df =
      spark.read
        .schema(getSchema())
        .csv("file:///C:\\Users\\mqlin\\Desktop\\testdata\\data.csv")
    //over(spark, df)
    rowNumOver(spark, df)
  }

  /**
    * 为每一行加上一个全部平均年龄列
    * 无开窗：单独算出平均分 ：x  。然后给每一行数据加一列
    * 开窗，一个over
    */
  def over(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("test")
    val sql = s"select *,avg(age) over() as avg_Age from test "
    //spark.sql(sql).show
    df.withColumn("avg_Age", avg("age").over()).show
  }

  /**
    *按name 分组，然后在分组内按 age 从大到小排序，得出每个分组的排序情况
    * 如果要取topn的话，外面再套一个select where top_id <= n 来实现topn的功能
    * @param spark
    * @param df
    */
  def rowNumOver(spark: SparkSession, df: DataFrame) {
    df.createOrReplaceTempView("test")
    val sql =
      """select *,
        |row_number() over (partition by name order by age desc) as top_id 
        |from test """.stripMargin
    spark.sql(sql).show

//    df.withColumn(
//        "top_id",
//        row_number().over(Window.partitionBy("name").orderBy(desc("age"))))
//      .show
  }

  /**
    * 其他开窗函数
    */
  println(
    "rank（）跳跃排序，有两个第二名时后边跟着的是第四名\n" +
      "dense_rank() 连续排序，有两个第二名时仍然跟着第三名\n" +
      "over（）开窗函数：\n" +
      "       在使用聚合函数后，会将多行变成一行，而开窗函数是将一行变成多行；\n" +
      "       并且在使用聚合函数后，如果要显示其他的列必须将列加入到group by中，\n" +
      "       而使用开窗函数后，可以不使用group by，直接将所有信息显示出来。\n" +
      "        开窗函数适用于在每一行的最后一列添加聚合函数的结果。\n" +
      "常用开窗函数：\n" +
      "   1.为每条数据显示聚合信息.(聚合函数() over())\n" +
      "   2.为每条数据提供分组的聚合函数结果" +
      "          (聚合函数() over(partition by 字段) as 别名) \n" +
      "         --按照字段分组，分组后进行计算\n" +
      "   3.与排名函数一起使用(row number() over(order by 字段) as 别名)\n" +
      "常用分析函数：（最常用的应该是1.2.3 的排序）\n" +
      "   1、row_number() over(partition by ... order by ...)\n" +
      "   2、rank() over(partition by ... order by ...)\n" +
      "   3、dense_rank() over(partition by ... order by ...)\n" +
      "   4、count() over(partition by ... order by ...)\n" +
      "   5、max() over(partition by ... order by ...)\n" +
      "   6、min() over(partition by ... order by ...)\n" +
      "   7、sum() over(partition by ... order by ...)\n" +
      "   8、avg() over(partition by ... order by ...)\n" +
      "   9、first_value() over(partition by ... order by ...)\n" +
      "   10、last_value() over(partition by ... order by ...)\n" +
      "   11、lag() over(partition by ... order by ...)\n" +
      "   12、lead() over(partition by ... order by ...)\n" +
      "lag 和lead 可以 获取结果集中，按一定排序所排列的当前行的上下相邻" +
      "                若干offset 的某个行的某个列(不用结果集的自关联）；\n" +
      "lag ，lead 分别是向前，向后；\n" +
      "lag 和lead 有三个参数，第一个参数是列名，" +
      "第二个参数是偏移的offset，" +
      "第三个参数是 超出记录窗口时的默认值\n" +
      "UDAF 函数  多对一\n" +
      "UDF  函数  一对一\n" +
      "开窗函数   一对多\n")
}
