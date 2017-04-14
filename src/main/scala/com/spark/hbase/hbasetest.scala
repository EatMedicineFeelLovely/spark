package com.spark.hbase

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import util.Properties
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.ConnectionFactory

object hbasetest {

    var zookeeper = "192.168.0.245,192.168.0.246,192.168.0.247"
    var conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeper)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent","/hbase")
    val admin=ConnectionFactory.createConnection(conf).getAdmin
    
    
    def main(args: Array[String]) {
      val tablename="table001"
      val tablenames=Array("table001","table002")
      val rowkey="rowkey001"
      val columnnames=Array("columnname001","cn002")
      val columndatas=Array("columndata001","data001")
      createHbaseTable(tablenames)
      println(">>>>>>>>>>>>>")
      putHbaseData(tablename, rowkey, columnnames, columndatas)
      println("1>>>>>>>>>>>>>")
      getHbaseData(tablename, rowkey)
    }

    // list the tables
    //val listtables=admin.listTables()
    //listtables.foreach(println)

    def createHbaseTable(tablenames: Array[String]) {
      for(tablename<-tablenames){
//      if (admin.tableExists(tablename)!=null){
          val tableDesc = new HTableDescriptor(Bytes.toBytes(tablename))
          val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("info"))
          tableDesc.addFamily(idsColumnFamilyDesc)
          admin.createTable(tableDesc)
//        }
      }
    }
  
    def putHbaseData(tablename: String,rowkey:String,columnnames:Array[String],columndatas:Array[String]) {
        val table = new HTable(conf, tablename)
        val theput= new Put(Bytes.toBytes(rowkey))
        for(a<-0 to columnnames.length){
        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes(columnnames(a)),Bytes.toBytes(columndatas(a)))
        table.put(theput)
      }
    }

    // let's insert some data in 'mytable' and get the row
    def getHbaseData(tablenames: String,rowkey: String):String={
    val table = new HTable(conf, tablenames)
    val theget= new Get(Bytes.toBytes(rowkey))
    val result=table.get(theget)
    val value=result.value().toString
//    println(Bytes.toString(value))
    value
  }
admin.close()
conf.clear()

}