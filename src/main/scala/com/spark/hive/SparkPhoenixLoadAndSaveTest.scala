package com.spark.run

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.phoenix.spark._

object SparkPhoenixLoadAndSaveTest {
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  var sparkconf: SparkConf = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  def main(args: Array[String]): Unit = {
    sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test Phoenix"))
    sqlContext = new SQLContext(sc)
    //loadPhoenixDF
    loadPhoenixDF
    //saveDFToPhoeni
  }
  def loadPhoenixDF() {
    //获取全表
    var phoenixDF = sqlContext.load("org.apache.phoenix.spark",
      Map("table" -> "US_POPULATION", "zkUrl" -> "192.168.10.191:2181"))

    phoenixDF.show
    //phoenixDF.select("CITY").show

    /*phoenixDF.filter(phoenixDF("COL1") === "test_row_1" && phoenixDF("ID") === 1L)
  .select(phoenixDF("ID"))
  .show*/

    //获取指定的列
    var phoenixDF2 = sqlContext.phoenixTableAsDataFrame("US_POPULATION",
      Seq("CITY", "POPULATION"),
      zkUrl = Some("192.168.10.191:2181"))
    phoenixDF2.foreach { x => println(x) }

    /* phoenixDF2.registerTempTable("tablename")
      phoenixDF2.map { x => x}  */

  }
  def saveDFToPhoeni() {
    //将一个RDD存进Phoenix
    val dataSet = List(("CB", "A", 11), ("CC", "B", 22), ("CD", "C", 33))
    sc.parallelize(dataSet)
      .saveToPhoenix("US_POPULATION",
        Seq("STATE", "CITY", "POPULATION"),
        zkUrl = Some("192.168.10.191"))
    //将一个DataFram存进Phoenix
    /*var phoenixDF=sqlContext.load("org.apache.phoenix.spark",
                                  Map("table" -> "TABLE1", "zkUrl" -> "phoenix-server:2181"))
 phoenixDF.save("org.apache.phoenix.spark", 
     SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE",
     "zkUrl" -> "phoenix-server:2181"))
*/

  }

}