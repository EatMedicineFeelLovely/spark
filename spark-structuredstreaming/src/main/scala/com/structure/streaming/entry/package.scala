package com.structure.streaming

import org.apache.spark.sql.types.StructType

package object entry {
  val userSchema = new StructType()
    .add("str", "string")
    .add("value", "int")

  //df.map(func)(RowEncoder(userSchema))//两种方式，上面那个方便，只要定义case class
//  val fun= { (r:Row) =>
//    val arr = r.getAs[String]("value").split(",")
//    val date = arr(0)
//    val site = arr(14)
//    Row(date, site)//这个的顺序跟StructType (userSchema)的顺序一一对应
//  }
  val kafkabroker = "localhost:9092"
  val topics = "test"
  val clickTopics = "mobileadsclicklog"

  val testClickTopic = "test_click"
  val testImpressTopic = "test_impress"
}
