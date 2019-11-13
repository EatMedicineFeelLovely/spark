package com.spark.udf.loader

import scala.collection.mutable

trait ClassLoaderTrait {
  var hasLoadClass: mutable.HashMap[String, Boolean]

}
