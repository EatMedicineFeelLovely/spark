package com.spark.sparkSql

object CaseClassUtil extends Serializable{

case class User(name:String,age:Int,phone:String)

case class Address(name:String,address:String,phone:String)
case class Detail(name:String,phone:String)

case class Table1(name:String,age:Int,address:String)
case class Table2(name:String,age:Int) 

case class HiveTempTable(id:Int,name:String) 
}