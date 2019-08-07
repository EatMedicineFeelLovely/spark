package org.apache.spark.sql

import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.execution.datasources.json.{TextInputJsonDataSource, TextInputJsonDataSourceTest}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

object DataFramReaderTest {
  private val extraOptions =
    new scala.collection.mutable.HashMap[String, String]
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

    verifyColumnNameOfCorruptRecord(schema,
                                    parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(
        schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val createParser = CreateJacksonParser.string _
    val parsed = jsonDataset.rdd.mapPartitions { iter =>
      val rawParser = new JacksonParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input, createParser, UTF8String.fromString),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord,
        parsedOptions.multiLine
      )
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed,
                                         schema,
                                         isStreaming = jsonDataset.isStreaming)
  }
}
