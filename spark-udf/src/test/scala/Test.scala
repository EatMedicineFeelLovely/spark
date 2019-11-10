import com.spark.udf.register.impl.HdfsJarUDFRegister

object Test {
  def main(args: Array[String]): Unit = {

    val udfManager = new HdfsJarUDFRegister("eeee", Map("q" -> "w"))

  }
}
