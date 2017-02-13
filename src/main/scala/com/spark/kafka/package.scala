package com.spark

import org.apache.spark.rdd.RDD
package object kafka {
  implicit def writeDataToKafka2[T](rdd: RDD[T])=new RDDKafkaWriter(rdd)
  
}