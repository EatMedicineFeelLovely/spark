package com.spark.ml

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.linalg.DenseMatrix
import breeze.linalg._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
object TestVector {
  def main(args: Array[String]): Unit = {
    //创建一个稠密矩阵
    var a = Vectors.dense(1.0, 2.0, 3.0)

    var b = Vectors.dense(1.0, 2.0, 3.0)
    var a2=new SparseVector(1,Array(0, 1, 2),Array(1.0, 2.0, 3.0))
    //a2.dot(a2)
    var a1=new DenseVector(Array(1.0, 2.0, 3.0))
    //a1.dot(b)
   // b.toDense.dot(a.toDense)
  }
}