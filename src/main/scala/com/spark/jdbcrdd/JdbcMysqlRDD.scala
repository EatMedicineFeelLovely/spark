package com.spark.jdbcrdd

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.ResultSet
import java.sql.Connection
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
class JdbcMysqlPartition(idx: Int, val startId: Long, val perPartitionNum: Long) extends Partition {
  override def index = idx
}
class JdbcMysqlRDD[T:ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql:String,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcMysqlRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging{
  override def count()=getRowsNum(sql)
  override def getPartitions: Array[Partition] = {
    val rowsNum=getRowsNum(sql)
    //Each partition limit on the number of article
    val perPartitionNum=rowsNum/numPartitions
    //Add the remaining to the last partition
    val lastPartitionNum=perPartitionNum+(rowsNum%numPartitions)
    (0 until numPartitions).map(i => {
    	val start = (i*perPartitionNum)
    	if(i==(numPartitions-1)){
    	  new JdbcMysqlPartition(i, start, lastPartitionNum)
    	}else
        new JdbcMysqlPartition(i, start, perPartitionNum)
    }).toArray
  }
  /**
   * For how many records
   * @param The SQL query
   */
  def getRowsNum(sql:String)={
    var rowsNum=0
     var tmpConn=getConnection()
     try{
     if(sql.toLowerCase.indexOf("from")<0){
       logError(" sql is error , There must be the from keyword ")
     }else{
       val nsql="select count(1) "+sql.substring(sql.toLowerCase.indexOf("from"), sql.size)
       val stmt = tmpConn.prepareStatement(nsql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
       
       val rs = stmt.executeQuery()
       if(rs.next()){
         rowsNum=rs.getInt(1)
       }
       stmt.close()
     }
     }catch {
       case t: Throwable => t.printStackTrace() // TODO: handle error
     }finally {
    	 tmpConn.close()
    	 tmpConn=null
     }
    rowsNum
  }
  //每个分区怎么获取数据的原理是按照分页的原理来取的
   override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val part = thePart.asInstanceOf[JdbcMysqlPartition]
    val conn = getConnection()
    val partSql=sql+" limit ?,?"
    val stmt = conn.prepareStatement(partSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
   if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }
    stmt.setLong(1, part.startId)
    stmt.setLong(2, part.perPartitionNum)
    val rs = stmt.executeQuery()
    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
   
   
   
   
}
object JdbcMysqlRDD{
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
}
abstract class NextIterator[U] extends Iterator[U] {
  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false
  protected def getNext(): U
  protected def close()
  def closeIfNeeded() {
    if (!closed) {
      closed = true
      close()
    }
  }
  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }
  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}
