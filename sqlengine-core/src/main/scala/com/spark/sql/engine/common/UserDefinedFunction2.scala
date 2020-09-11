package com.spark.sql.engine.common

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

case class UserDefinedFunction2(inputParamsNum: Int,
                                f: AnyRef,
                                dataType: DataType,
                                inputTypes: Option[Seq[DataType]])
