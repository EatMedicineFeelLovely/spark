package com.test

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Base64
object HbaseUtil {
  var hbaseConn: Connection = null
  var zookeeper:String = "cdh-master,node1,node2"
  def main(args: Array[String]): Unit = {
    initHbaseConn
    getKyLinHbaseData
    
    
    
  }
  def getKyLinHbaseData(){
    val table = hbaseConn.getTable(TableName.valueOf("KYLIN_HT7HUTOKSO"))
    val scan = new Scan()
    scan.setMaxResultSize(10L)
    scan.setMaxResultsPerColumnFamily(1)
    val resultScanner = table.getScanner(scan);
     for (result <- resultScanner) {
      var listCells = result.listCells()
      
      /*for (cell <- listCells) {
        var column = new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
       println(column) 
        
        //rowMap.put(column, new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
      }*/
    }
    table.close()
  }
    def initHbaseConn {
    if(hbaseConn!=null) hbaseConn.close()
    hbaseConn=null
    var hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zookeeper)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConn = ConnectionFactory.createConnection(hconf)
  }
}