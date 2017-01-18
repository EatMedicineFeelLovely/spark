package com.spark.streaming

object DataProducter {
  def main(args: Array[String]): Unit = {
   val conn= getConnection()
   var id=1;
   var sql="insert into test(id,name) values"
   while(true){
   val values=(id to id+2).map{x=>
       "("+x+",'"+x+"')"
     }.mkString(",")
     val nsql=sql+values
     println(nsql)
     id+=3
     Thread.sleep(8000)
     val statement = conn.prepareStatement(nsql);
     statement.executeUpdate(); 
   }
   
   
  }
  
}