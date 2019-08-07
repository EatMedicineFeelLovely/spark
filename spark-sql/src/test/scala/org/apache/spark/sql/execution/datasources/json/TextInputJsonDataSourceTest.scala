package org.apache.spark.sql.execution.datasources.json

import com.fasterxml.jackson.core.JsonFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.Text
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser, JsonInferSchema, JsonInferSchemaTest}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object TextInputJsonDataSourceTest extends JsonDataSource {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    true
  }

  override def infer(
                      sparkSession: SparkSession,
                      inputPaths: Seq[FileStatus],
                      parsedOptions: JSONOptions): StructType = {
    val json: Dataset[String] = createBaseDataset(sparkSession, inputPaths, parsedOptions)

    inferFromDataset(json, parsedOptions)
  }

  def inferFromDataset(json: Dataset[String], parsedOptions: JSONOptions): StructType = {
    val sampled: Dataset[String] = JsonUtils.sample(json, parsedOptions)
    val rdd: RDD[InternalRow] = sampled.queryExecution.toRdd
    val rowParser = parsedOptions.encoding.map { enc =>
      CreateJacksonParser.internalRow(enc, _: JsonFactory, _: InternalRow)
    }.getOrElse(CreateJacksonParser.internalRow(_: JsonFactory, _: InternalRow))

    SQLExecution.withSQLConfPropagated(json.sparkSession) {
      JsonInferSchemaTest.infer(rdd, parsedOptions, rowParser)
    }
  }

  private def createBaseDataset(
                                 sparkSession: SparkSession,
                                 inputPaths: Seq[FileStatus],
                                 parsedOptions: JSONOptions): Dataset[String] = {
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = inputPaths.map(_.getPath.toString),
        className = classOf[TextFileFormat].getName,
        options = parsedOptions.parameters
      ).resolveRelation(checkFilesExist = false))
      .select("value").as(Encoders.STRING)
  }

  override def readFile(
                         conf: Configuration,
                         file: PartitionedFile,
                         parser: JacksonParser,
                         schema: StructType): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
    val textParser = parser.options.encoding
      .map(enc => CreateJacksonParser.text(enc, _: JsonFactory, _: Text))
      .getOrElse(CreateJacksonParser.text(_: JsonFactory, _: Text))

    val safeParser = new FailureSafeParser[Text](
      input => parser.parse(input, textParser, textToUTF8String),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord,
      parser.options.multiLine)
    linesReader.flatMap(safeParser.parse)
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }
}
