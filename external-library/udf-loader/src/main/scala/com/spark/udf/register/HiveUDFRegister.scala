package com.spark.udf.register

import com.spark.udf.bean.UDFClassInfo
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

class HiveUDFRegister(val jarsPath: Seq[String], val func: Map[String, String])
    extends UDFRegisterTrait {
  var loadClassNames: Set[String] = _

  /**
    * 注册hive得func
    * @param spark
    * @param _log
    */
  def registerUDF(spark: SparkSession)(
      _log: Logger): Map[String, UDFClassInfo] = {
    func.foreach {
      case (funcName, funcClass) =>
        val sql = s"create temporary function $funcName as '$funcClass'"
        _log.info(sql)
        spark.sql(sql)
    }
    null
  }

  override def register()(_log: Logger): Map[String, UDFClassInfo] = ???

  /**
    * 比较两个是否为同一个
    * @param obj
    * @return
    */
  override def equalsOtherRegister(obj: Any): Boolean = {
    if (obj.isInstanceOf[HiveUDFRegister]) {
      val other = obj.asInstanceOf[HiveUDFRegister]
      other.jarsPath.equals(jarsPath) && func.equals(other.func)
    } else false
  }
  override def classHashCode(): Int = {
    func.hashCode()
  }
}
