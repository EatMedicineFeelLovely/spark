package com.spark.learn

import com.spark.learn.bean.Config
import scopt.OptionParser

package object entry {

  /**
    *脚本参数
    * @param args
    */
  def getParamConf(args: Array[String]) = {
    val parses = new OptionParser[Config](programName = "test") {
      head("test")
      opt[String]("path")
        .abbr("p")
        .text("文件路径")
        .action((x, c) => c.copy(path = x))
      opt[String]("day")
        .abbr("d")
        .text("某一天")
        .action((x, c) => c.copy(day = x))
    }
    val conf = parses.parse(args, Config()) match {
      case Some(conf) => conf
      case _          => println("error "); sys.exit(1)
    }
    conf
  }
}
