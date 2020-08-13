package com.spark.learn.sql.test

import com.spark.code.udt.{HyperLogLog, RegisterSet}
import org.apache.spark.sql.types.udt.HyperLogLog2
import org.backuity.clist.Command
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class OtherTest extends FunSuite with BeforeAndAfterAll {
  def log2m(rsd: Double): Int =
    (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2)).toInt

  /**
   *
   */
  test("HyperLogLog2") {
    //    val a = new HyperLogLog2(log2m(0.05))
    //    a.offer("d1")
    //    a.offer("d2")
    //    a.offer("d3")
    //    a.offer("d1")
    //
    //    val uv1 = a.cardinality()
    //    println(uv1) //3
    //
    //    val b = new HyperLogLog2(log2m(0.05))
    //    b.offer("d4")
    //    b.offer("d2")
    //    b.offer("d5")
    //    b.offer("d1")
    //    val uv2 = b.cardinality()
    //    println(uv2) // 4
    //
    //    val b_back = HyperLogLog.Builder.build(b.getBytes)
    //    println(b_back.cardinality()) // 4
    //    a.addAll(b_back) // 5
    //    println(a.cardinality())
  }

  import org.backuity.clist._
  import org.backuity.clist.util.Read

  test("org.backuity.clist") {
    Cli.parse(Array("--number-nonblank='hello-world'", "--params=key1=value1,key2=value2")).withCommand(new Cat) { case cat =>
      // the new Cat instance will be mutated to receive the command-line arguments
      println(cat.numberNonblank, cat.params.get("key1"))
    }
  }

  case class Paramer(str: String) {
    val map = str.split(",").map(x => {
      val Array(k, v) = x.split("="); (k -> v)
    }).toMap
    def get(key: String): Option[String] = {
      map.get(key)
    }
  }

  implicit val paramRead = Read.reads[Paramer]("") {
    str => {
      Paramer(str)
    }
  }

  class Cat extends Command(description = "concatenate files and print on the standard output") {
    // `opt`, `arg` and `args` are scala macros that will extract the name of the member
    // to use it as the option/arguments name.
    // Here for instance the member `showAll` will be turned into the option `--show-all`
    var showAll = opt[Boolean](abbrev = "A", description = "equivalent to -vET", default = false)

    // an abbreviated form can be added, so that this option can be triggered both by `--number-nonblank` or `-b`
    var numberNonblank = opt[String](description = "number nonempty output lines, overrides -n", default = "jj")

    // default values can be provided
    var maxLines = opt[Int](default = 123)
    var params = arg[Paramer](description = "files to concat")
  }

}
