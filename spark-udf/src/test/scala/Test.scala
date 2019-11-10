import com.spark.udf.register.common.UdfRegisterManager

object Test {
  def main(args: Array[String]): Unit = {

    val udfManager = new UdfRegisterManager(Map("q" -> "w"))("eeee")

  }
}
