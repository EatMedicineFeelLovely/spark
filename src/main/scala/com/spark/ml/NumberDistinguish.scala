package com.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes

object NumberDistinguish {
  def main(args: Array[String]): Unit = {
    val parentPath = "C:\\Users\\zhiziyun\\Desktop\\ml\\jpgint.txt"
    val conf = new SparkConf()
      .setAppName("DigitRecgonizer")
      .setMaster("local[10]")
      .set("spark.driver.memory", "10G")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile(parentPath, 10)
    val records = rawData.map(line => line.split(","))
    val data = records.map { r =>
      val label = r(0).toInt
      val features = r.slice(1, r.size).map(p => p.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }
    val nbModel = NaiveBayes.train(data)
    val nbTotalCorrect = data.map { point =>
      if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val numData = data.count()
    val nbAccuracy = nbTotalCorrect / numData
    println(nbAccuracy)
  }
}