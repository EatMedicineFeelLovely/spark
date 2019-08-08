import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  MapType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFramReaderTest, SparkSession}

object Test {
  val schema: StructType = {
    new StructType()
      .add(StructField("serdatetime", StringType))
      .add(StructField("clientip", StringType))
      .add(StructField("clienttime", StringType))
      .add(StructField("product", StringType))
      .add(StructField("plat", IntegerType))
      .add(StructField("deviceid", StringType))
      .add(StructField("type", StringType))
      .add(StructField("mac", StringType))
      .add(StructField("model", StringType))
      .add(StructField("duid", StringType))
      .add(StructField("imei", StringType))
      .add(StructField("serialno", StringType))
      .add(StructField("networktype", StringType))
      .add(StructField("appkey", StringType))
      .add(StructField("commonsdkver", IntegerType))
      .add(StructField("apppkg", StringType))
      .add(StructField("appver", StringType))
      .add(StructField("sysver", StringType))
      .add(StructField("factory", StringType))
      .add(StructField("language", StringType))
      .add(StructField("dc", IntegerType))
      .add(StructField("ua", StringType))
      .add(StructField("sdks", ArrayType(MapType(StringType, StringType))))
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("aa")
      .getOrCreate()
    val st = System.currentTimeMillis()
    import spark.implicits._
    val ss = spark.read.textFile("C:\\Users\\mqlin\\Desktop\\pv\\pv2")
    val r = ss.select(from_json($"value".cast(StringType), schema).as("value")).select("value.*")
    //val r = spark.read.option("samplingRatio",0.0001).json(ss)

//val r = DataFramReaderTest.json(ss,spark)
//    println(r.schema)
//    r.show()

    println(r.schema)
    r.show
    val eb = System.currentTimeMillis()
    println(eb - st)
  }

  def a(): (Any, Any) => Int = {
    case (ss: Int, cc: Int)    => ss + cc
    case (ss: String, cc: Int) => a()(ss.toString, 2)
  }

}
