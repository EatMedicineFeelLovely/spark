package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, DropMalformedMode, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class FailureSafeParserTest[IN](
  rawParser: IN => Seq[InternalRow],
  mode: ParseMode,
  schema: StructType,
  columnNameOfCorruptRecord: String,
  isMultiLine: Boolean) {

  private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) {
      (row, badRecord) => {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        resultRow
      }
    } else {
      (row, _) => row.getOrElse(nullResult)
    }
  }

  private val skipParsing = !isMultiLine && mode == PermissiveMode && schema.isEmpty

  def parse(input: IN): Iterator[InternalRow] = {
    try {
      if (skipParsing) {
        Iterator.single(InternalRow.empty)
      } else {
        rawParser.apply(input).toIterator.map(row => toResultRow(Some(row), () => null))
      }
    } catch {
      case e: BadRecordException => mode match {
        case PermissiveMode =>
          Iterator(toResultRow(e.partialResult(), e.record))
        case DropMalformedMode =>
          Iterator.empty
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Parse Mode: ${FailFastMode.name}.", e.cause)
      }
    }
  }
}

