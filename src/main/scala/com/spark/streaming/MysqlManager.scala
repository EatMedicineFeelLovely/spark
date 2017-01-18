package org.apache.spark.streaming.mysql

import org.apache.spark.streaming.StreamingContext
import java.sql.Connection
import java.sql.ResultSet
import scala.reflect.ClassTag

object MysqlManager {
  def creatMysqlInputStream[T:ClassTag](
    @transient ssc_ : StreamingContext,
    getConnection: () => Connection,
    tablename: String,
    idcloumn:String,
    lowerBound: Long,
    sql:String,
    numPartitions: Int,
    mapRow: (ResultSet) => T)={
    new DirectMysqlInputDStream(ssc_,getConnection,tablename,idcloumn,lowerBound,sql,numPartitions,mapRow)
  }
}