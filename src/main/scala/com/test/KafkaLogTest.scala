package com.test

import org.slf4j.Logger
import org.slf4j.LoggerFactory


object KafkaLogTest {
  def main(args: Array[String]): Unit = {
    var LOGGER: Logger = LoggerFactory.getLogger("KafkaTest")//日志记录
    for (i <- 1 to 1000) {
            LOGGER.info("Info [" + i + "]");
            println("Info [" + i + "]")
            Thread.sleep(1000);
        }
  }
}