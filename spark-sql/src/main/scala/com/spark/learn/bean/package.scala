package com.spark.learn

package object bean {

  /**
    *
    * @param k
    * @param v
    */
  case class kv(k: Int, v: Int) extends AbstractBasicsCaseClass[kv]

  /**
    *命令行配置信息， 采用 --path   --day  的方式来传参
    * @param path
    * @param day
    */
  case class Config(path: String = "defaultPath",
                    day: String = "defaultDay")
      extends AbstractBasicsCaseClass[Config]

}
