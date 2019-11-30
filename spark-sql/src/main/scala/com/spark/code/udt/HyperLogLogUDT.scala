package org.apache.spark.sql.types.udt

import java.io.{ByteArrayInputStream, DataInputStream}

import com.clearspring.analytics.util.Bits
import com.spark.code.udt.{HyperLogLog, RegisterSet}
import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}

@SQLUserDefinedType(udt = classOf[HyperLogLogUDT])
case class HyperLogLog2(k: Int, registerSets: RegisterSet)
    extends HyperLogLog(k, registerSets) {}

class HyperLogLogUDT extends UserDefinedType[HyperLogLog2] {

  override def sqlType: DataType = BinaryType

  override def pyUDT: String = "pyspark.testing.sqlutils.ExamplePointUDT"

  override def serialize(p: HyperLogLog2) = {
    p.getBytes
  }
  override def deserialize(datum: Any): HyperLogLog2 = {
    val bais = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
    val serializedByteStream = new DataInputStream(bais)
    val log2m = serializedByteStream.readInt
    val byteArraySize = serializedByteStream.readInt
    val r = new RegisterSet(1 << log2m,
                            Bits.getBits(serializedByteStream, byteArraySize))
    new HyperLogLog2(log2m, r)
  }

  override def userClass: Class[HyperLogLog2] = classOf[HyperLogLog2]

  private[spark] override def asNullable: HyperLogLogUDT = this
}
