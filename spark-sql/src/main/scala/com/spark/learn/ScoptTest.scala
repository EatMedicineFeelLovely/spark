package com.spark.learn

import com.spark.learn.cassclass.AbstractBasicsCaseClass
import scopt.OptionParser

/**
  * @author ${user.name}
  */
object ScoptTest {
  case class Config(path: String = "defaultPath",
                    topn: String = "defaultTopN",
                    day: String = "defaultDay")
      extends AbstractBasicsCaseClass[Config]

  def main(args: Array[String]): Unit = {
    val parses = new OptionParser[Config](programName = "test") {
      head("test")
      opt[String]("parh")
        .abbr("p")
        .text("文件路径")
        .action((x, c) => c.copy(path = x))

      opt[String]("topn")
        .abbr("t")
        .text("topn")
        .action((x, c) => c.copy(topn = x))

      opt[String]("day")
        .abbr("d")
        .text("某一天")
        .action((x, c) => c.copy(day = x))
    }
    parses.parse(args, Config()) match {
      case Some(conf) => println(conf)
      case _          => println("error "); sys.exit(1)
    }

  }

}
