package com.spark.kafka

import kafka.serializer.Encoder
import java.util.HashMap
import kafka.serializer.StringEncoder


class HashMapEncoder extends Encoder[HashMap[String,Any]]{
  @Override
def toBytes(a:HashMap[String,Any])= {
    
    null
}
}