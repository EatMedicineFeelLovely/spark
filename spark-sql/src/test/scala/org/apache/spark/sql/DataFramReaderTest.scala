package org.apache.spark.sql

import org.apache.spark.sql.catalyst.json.{
  CreateJacksonParser,
  JSONOptions,
  JacksonParser
}
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.execution.datasources.json.{
  FailureSafeParserTest,
  TextInputJsonDataSource,
  TextInputJsonDataSourceTest
}
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  MapType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

object DataFramReaderTest {
  private val extraOptions =
    new scala.collection.mutable.HashMap[String, String]

  extraOptions.put("samplingRatio", "0.0001")
  private var userSpecifiedSchema: Option[StructType] = None
  private def verifyColumnNameOfCorruptRecord(
      schema: StructType,
      columnNameOfCorruptRecord: String): Unit = {
    schema.getFieldIndex(columnNameOfCorruptRecord).foreach {
      corruptFieldIndex =>
        val f = schema(corruptFieldIndex)
        if (f.dataType != StringType || !f.nullable) {
          throw new AnalysisException(
            "The field for corrupt records must be string type and nullable")
        }
    }
  }
  def json(jsonDataset: Dataset[String],
           sparkSession: SparkSession): DataFrame = {
    val parsedOptions = new JSONOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val schema = userSpecifiedSchema.getOrElse {
      TextInputJsonDataSourceTest.inferFromDataset(jsonDataset, parsedOptions)
    }
//    val schema: StructType = {
//      new StructType()
//        .add(StructField("serdatetime", StringType))
//        .add(StructField("clientip", StringType))
//        .add(StructField("clienttime", StringType))
//        .add(StructField("product", StringType))
//        .add(StructField("plat", IntegerType))
//        .add(StructField("deviceid", StringType))
//        .add(StructField("type", StringType))
//        .add(StructField("mac", StringType))
//        .add(StructField("model", StringType))
//        .add(StructField("duid", StringType))
//        .add(StructField("imei", StringType))
//        .add(StructField("serialno", StringType))
//        .add(StructField("networktype", StringType))
//        .add(StructField("appkey", StringType))
//        .add(StructField("commonsdkver", IntegerType))
//        .add(StructField("apppkg", StringType))
//        .add(StructField("appver", StringType))
//        .add(StructField("sysver", StringType))
//        .add(StructField("factory", StringType))
//        .add(StructField("language", StringType))
//        .add(StructField("dc", IntegerType))
//        .add(StructField("ua", StringType))
//        .add(StructField("sdks", ArrayType(MapType(StringType, StringType))))
//    }
    verifyColumnNameOfCorruptRecord(schema,
                                    parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(
        schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val createParser = CreateJacksonParser.string _
    val parsed = jsonDataset.rdd.mapPartitions { iter =>
      val rawParser = new JacksonParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParserTest[String](
        input => rawParser.parse(input, createParser, UTF8String.fromString),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord,
        parsedOptions.multiLine
      )
      val r = parser.parse("""{"appver":"112331","sdks":"ss"}""")
      r.toList.foreach(println)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed,
                                         schema,
                                         isStreaming = jsonDataset.isStreaming)
  }
}
