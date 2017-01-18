package org.apache.spark.streaming.mysql

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.ResultSet
import java.sql.Connection
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.TaskContext
class JdbcSparkStreamPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index = idx
}
class JdbcSparkStreamRDD[T:ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    lowerBound: Long,
    upperBound: Long,
    rowkeyName:String,
    sql:String,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcSparkStreamRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging{
  //每个分区获取数据的
  override def getPartitions: Array[Partition] = {
    val length = 1 + upperBound - lowerBound
    (0 until numPartitions).map(i => {
    	val start = lowerBound + ((i * length) / numPartitions).toLong
    	val end = lowerBound + (((i + 1) * length) / numPartitions).toLong - 1
    	
      new JdbcSparkStreamPartition(i, start, end)
    	
    }).toArray
  }
  override def count()=getRowsNum(sql)
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
  //每个分区怎么获取数据的
   override def compute(thePart: Partition, context: TaskContext) = {
     val part = thePart.asInstanceOf[JdbcSparkStreamPartition]
     //如果这段时间没有数据，就返回空的
     if(part.lower>part.upper){
       Iterator.empty
     }
   else
    new JdbcIterator[T] {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val conn = getConnection()
    var parttionSql=if(sql.toLowerCase.contains("where")) sql+" and "+rowkeyName+" >= ? AND "+rowkeyName+" <= ?"
                    else sql+" where "+rowkeyName+" >= ? AND "+rowkeyName+" <= ?"
    val stmt = conn.prepareStatement(parttionSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
 if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }
    stmt.setLong(1, part.lower)
    stmt.setLong(2, part.upper)
    
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
   
   
   
}
object JdbcSparkStreamRDD{
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
}
abstract class JdbcIterator[U] extends Iterator[U] {
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
