package com.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
object SparkCSVTest {
   var sc: SparkContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
init
tets
  }
      
    def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
  def tets(){
val sqlContext = new SQLContext(sc)
val customSchema = StructType(Array(
    StructField("year", IntegerType, true),
    StructField("comment", IntegerType, true),
    StructField("blank", IntegerType, true)))

val df = sqlContext.load(
    "com.databricks.spark.csv",
    schema = customSchema,
    Map("path" -> "C:\\Users\\zhiziyun\\Desktop\\csvtest.csv", "header" -> "true"))

val selectedData = df.select("year", "comment")
selectedData.save("C:\\Users\\zhiziyun\\Desktop\\re.csv", "com.databricks.spark.csv")
  }
}