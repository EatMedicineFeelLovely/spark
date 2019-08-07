import org.apache.spark.sql.{DataFramReaderTest, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("aa")
      .getOrCreate()
    val ss = spark.read.textFile("C:\\Users\\Master\\Desktop\\aa.txt")

    val r = DataFramReaderTest.json(ss,spark)
    println(r.schema)
    r.show()
  }

  def a(): (Any, Any) => Int = {
    case (ss: Int, cc: Int)    => ss + cc
    case (ss: String, cc: Int) => a()(ss.toString, 2)
  }


}
