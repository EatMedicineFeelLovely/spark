package com.spark.udf.bean

case class PreCompileInfo(classPath: String,
                          compileName: String,
                          methodNames: String,
                          code: String = null) {}
