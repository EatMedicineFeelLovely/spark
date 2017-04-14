package org.apache.spark.streaming.mysql

import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import java.sql.ResultSet
import org.apache.spark.streaming.dstream.InputDStream
import java.sql.Connection
import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.RateController
class DirectMysqlInputDStream[T:ClassTag](
    @transient ssc_ : StreamingContext,
    getConnection: () => Connection,
    tablename: String,
    idcloumn:String,
    fromTime: Long,
    sql:String,
    numPartitions: Int,
    mapRow: (ResultSet) => T) extends InputDStream[T](ssc_) with Logging {
  //每个分区的获取条数限制
  val maxRows:Long = context.sparkContext.getConf.getInt("spark.streaming.mysql.maxRetries", 1) * numPartitions * context.graph.batchDuration.milliseconds.toLong /1000
  var currentOffsets=fromTime
  val mysqlConn=getConnection()
 // println(ssc_.conf)
   override def start(): Unit = {}
   override def stop(): Unit = {}
 // limits the maximum number of messages per partition
   protected def clamp(currentOffsets: Long): Long = {
    //获取最大的id
    val clampSql="select max("+idcloumn+") from "+tablename+" where "+
                  idcloumn+" >="+currentOffsets
    val stmt = mysqlConn.prepareStatement(clampSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val rs = stmt.executeQuery()
    //如果没有新数据就nextId<currentOffsets
    val nextId=if( rs.next()) rs.getInt(1) else currentOffsets-1
    if((nextId-currentOffsets)>maxRows) maxRows+currentOffsets else nextId
    
  }

   override def compute(validTime: Time): Option[JdbcSparkStreamRDD[T]] = {
   val nextId=clamp(currentOffsets)
   //如果没有新数据就nextId<currentOffsets
   var rdd=new JdbcSparkStreamRDD(context.sparkContext, getConnection,currentOffsets, 
			                                 nextId,idcloumn,sql, numPartitions,mapRow)
   
   currentOffsets=if(nextId<currentOffsets) currentOffsets else nextId+1
   println("nextID "+currentOffsets)
   Some(rdd)
   }
}