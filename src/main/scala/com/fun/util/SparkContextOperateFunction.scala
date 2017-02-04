
package com.fun.util
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import java.sql.Connection
import java.sql.ResultSet
import com.spark.rdd.JdbcMysqlRDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.mysql.MysqlManager
trait SparkContextOperateFunction {
  implicit class SparkContextFunc(sc:SparkContext){
    def hbaseRDD(tablename:String)=println("return hbase RDD")
    def mysqlRDD[T:ClassTag](createConnection: () => Connection,
        sql:String,numPartitions: Int,extractValues: (ResultSet) => T )
        =new JdbcMysqlRDD(sc, createConnection,sql,numPartitions,extractValues)
  }
  implicit class StreamingContextFunc(ssc:StreamingContext){
    def createDirectMysqlDStream[T:ClassTag](
    getConnection: () => Connection,
    tablename: String,
    idcloumn:String,
    fromTime: Long,
    sql:String,
    numPartitions: Int,
    extractValues: (ResultSet) => T)=
   MysqlManager.creatMysqlInputStream(ssc, getConnection, tablename,idcloumn, fromTime,sql, numPartitions, extractValues)
  }
}