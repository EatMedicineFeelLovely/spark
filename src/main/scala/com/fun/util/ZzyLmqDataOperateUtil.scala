package com.fun.util

import java.sql.DriverManager
import java.sql.ResultSet

trait ZzyLmqDataOperateUtil {
   def createConnection() = {  
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://192.168.10.159:3306/test", "root", "zhiziyun0628")  
    }  
   def extractValues(r: ResultSet) = {  
      (r.getString(1), r.getString(2),r.getString(3))
    } 
   def sscextractValues(r: ResultSet) = {  
      (r.getString(1), r.getString(2))
    }  
  def getConnection() = {  
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://192.168.10.159:3306/test", "root", "zhiziyun0628")  
    }  
}