package com.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Table

object PutDataToHbase {
  def main(args: Array[String]): Unit = {
    var hconf = HBaseConfiguration.create();
    hconf.set("hbase.zookeeper.quorum", "virtual-2,virtual-3,virtual-4");
    hconf.set("hbase.zookeeper.property.clientPort", "2181");
    var hconnection = ConnectionFactory.createConnection(hconf)
    var table = hconnection.getTable(TableName.valueOf("rt_mobilertbreport_bycreative"))
    putData(table,"WWTEY3i9OEh,hEmlg0eYmSk,2016-08-23")
    putData(table,"WWTEY3i9OEh,d2wns0wqJna,2016-08-23")
    putData(table,"0zoTLi29XRgq,istRh0Z1G4o,2016-08-23")
    putData(table,"WWTEY3i9OEh,hs8Xi0hvIbe,2016-08-23")
    
    println(">>>>>>>>>>")
  }
  def putData(table: Table, rowkey: String) {
    val p = new Put(Bytes.toBytes(rowkey))
    p.addColumn("info".getBytes, "additionalcpmcost".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "fee".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "deliveryUV".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "delivery".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "cpmcost".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "clicks".getBytes, "100".getBytes)
    p.addColumn("info".getBytes, "clickUV".getBytes, "100".getBytes)
    table.put(p)
  }
}