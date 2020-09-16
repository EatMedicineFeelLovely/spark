package com.spark.udf.register

import com.spark.udf.bean.{PreCompileInfo, UDFClassInfo}
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

class HiveUDFRegister(val spark: SparkSession, val jarsPath: Seq[String], val func: Map[String, String])
    extends UDFRegisterTrait {
  var loadClassNames: Set[String] = _
  // override def register()(_log: Logger): Map[String, UDFClassInfo] = ???
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

  /**
   * 注册进spark
   */
  override def registerUDF(): Map[String, UDFClassInfo] = {
    func.foreach {
      case (funcName, funcClass) =>
        val sql = s"create temporary function $funcName as '$funcClass'"
        spark.sql(sql)
    }
    null
  }

  override def getClassInstance(info: PreCompileInfo): Class[_] = null
}
