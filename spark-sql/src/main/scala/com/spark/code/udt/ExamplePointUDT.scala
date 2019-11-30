package org.apache.spark.sql.types.udt

import java.io.Serializable
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  DataType,
  DoubleType,
  SQLUserDefinedType,
  UserDefinedType
}

/**
  * An example class to demonstrate UDT in Scala, Java, and Python.
  *
  * @param x x coordinate
  * @param y y coordinate
  */
@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
case class ExamplePoint(val x: Double, val y: Double) extends Serializable {

  override def hashCode(): Int = 31 * (31 * x.hashCode()) + y.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: ExamplePoint => this.x == that.x && this.y == that.y
    case _                  => false
  }

  override def toString(): String = s"($x, $y)"
}

/**
  * User-defined type for [[ExamplePoint]].
  */
class ExamplePointUDT extends UserDefinedType[ExamplePoint] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def pyUDT: String = "pyspark.testing.sqlutils.ExamplePointUDT"

  override def serialize(p: ExamplePoint): GenericArrayData = {
    val output = new Array[Any](2)
    output(0) = p.x
    output(1) = p.y
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): ExamplePoint = {
    datum match {
      case values: ArrayData =>
        new ExamplePoint(values.getDouble(0), values.getDouble(1))
    }
  }

  override def userClass: Class[ExamplePoint] = classOf[ExamplePoint]

  private[spark] override def asNullable: ExamplePointUDT = this
}
