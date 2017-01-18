package com.spark.util

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.spark.util.SparkKryoSerializerTest.MygisterKryoClass

class SparkKryoRegistrators extends KryoRegistrator{
  @Override
	def registerClasses(kryo:Kryo) {
      kryo.register(classOf[String])
		  kryo.register(classOf[MygisterKryoClass])

	}
}