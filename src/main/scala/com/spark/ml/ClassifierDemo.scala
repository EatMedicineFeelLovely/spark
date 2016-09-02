package com.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
/**
 * 线性模型（逻辑回归，线性支持向量机SVM）
 *线性模型的核心思想是对样本的预测结果进行建模，即对输入变量（特征矩阵）应用简单的线性预测函数y=f（w*x）
 *线性预测函数是用来训练数据的（得出w权重矩阵），使用逻辑回归或者线性支持向量机SVM（损失函数）来得出预测值（传入特征矩阵和w）
 *根据精确度来判断使用哪个损失函数来得出预测值
 *x为输入的特征矩阵，y为值（预测值）
 *在训练模型的时候，y为实际值，x为特征，存在一个权重向量能够最小化所有训练样本的由损失函数计算出来的误差最小。（最终是要求一个w）
 * 1 1.1951146419526084 0.9947742549449248 0.19840725400812698 2.48569644222758 1.7391898607628944 
 * 第一个为结果值（分类） 后面为特征值 
 */
object ClassifierDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("ClassifierDemo")
      System.setProperty("hadoop.home.dir", "D:\\eclipse\\hdplocal2.6.0")
      val sc = new SparkContext(conf)
      var numIterations=10
    
      val data=sc.textFile("inputFile/lr_data.txt", sc.defaultMinPartitions)
      //提取特征向量
      val records=data.map { line => line.split(" ") }
      
      //logisticRegressionWithSGDModel(records,numIterations)
      //naiveBayesModel(records,numIterations)
      //svmWithSGDModel(records,numIterations)
      decisionTree(sc,numIterations)
      
      
  }
  /**
   * 逻辑回归模型
   */
  def logisticRegressionWithSGDModel(records:RDD[Array[String]],numIterations:Int){
    //清洗数据，取第一个为结果值，后面为特征值  
    val rawData=records.map { r => {
        val label=if(r(0).toInt<0) 0.0 else r(0).toInt
        val features=r.slice(1, r.size-1).map (_.toDouble)
        LabeledPoint(label,Vectors.dense(features))}
      }
    val lrModel=LogisticRegressionWithSGD.train(rawData, numIterations)
     //计算准确率 
    val lrAccuracy=rawData.map { point => {
      if(lrModel.predict(point.features) == point.label) 1 else 0  
    }
    }.sum()/rawData.count()
    println("准确率："+lrAccuracy)
   //
    
   /* //传入一个特征矩阵来预测这个产品是属于哪一类的,结果为0或者1
    val prediction=lrModel.predict(rawData.map { data => data.features }).collect()
    //实际的结果值
    val label=rawData.map { x => x.label }.collect()
    
    for(i<- 0 until label.length){
      if(prediction.apply(i)==label.apply(i)){
        println("预测："+prediction.apply(i)+"->>> 实际："+label.apply(i))  
      }else
      {
         println("预测："+prediction.apply(i)+"@@@@@ 实际："+label.apply(i))  
      } 
    }*/
  }
  
  /**
   * 朴素贝叶斯模型（特征值不允许为负）
   * map{x=> if(x.toDouble<0) 0.0 else x.toDouble}
   */
  def naiveBayesModel(input:RDD[Array[String]],numIterations:Int){
    val rawData=input.map { r => {
        val label=if(r(0).toInt<0) 0.0 else r(0).toInt
        val features=r.slice(1, r.size-1).map{x=> if(x.toDouble<0) 0.0 else x.toDouble}
        LabeledPoint(label,Vectors.dense(features))}
      }
    val nbModel=NaiveBayes.train(rawData,numIterations)
   
    //计算准确率 
    val nbAccuracy=rawData.map { point => {
      if(nbModel.predict(point.features) == point.label) 1 else 0  
    }
    }.sum()/rawData.count()
    println("准确率："+nbAccuracy)
   //
    
/*    
    val prediction=nbModel.predict(rawData.map { data => data.features }).collect()
    //实际的结果值
    val label=rawData.map { x => x.label }.collect()
    
    for(i<- 0 until label.length){
      if(prediction.apply(i)==label.apply(i)){
        println("预测："+prediction.apply(i)+"->>> 实际："+label.apply(i))  
      }else
      {
         println("预测："+prediction.apply(i)+"@@@@@ 实际："+label.apply(i))  
      } 
    }*/
    
    
  }
  
  /**
   * SVM模型
   */
  def svmWithSGDModel(input:RDD[Array[String]],numIterations:Int){
    val rawData=input.map { r => {
        val label=if(r(0).toInt<0) 0.0 else r(0).toInt
        val features=r.slice(1, r.size-1).map(_.toDouble)
        LabeledPoint(label,Vectors.dense(features))}
      }
    val svmModel=SVMWithSGD.train(rawData,numIterations)
   
    //计算准确率 
    val svmAccuracy=rawData.map { point => {
      if(svmModel.predict(point.features) == point.label) 1 else 0  
    }
    }.sum()/rawData.count()
    println("准确率："+svmAccuracy)
   //
  }
  /**
   * 决策树
   */
  def decisionTree(sc:SparkContext,numIterations:Int){
    val data=sc.textFile("inputFile/sample_tree_data.csv", sc.defaultMinPartitions)
      //提取特征向量
      val records=data.map { line => line.split(",") }
      val rawData=records.map { r => {
        val label=r(0).toInt
        val features=r.slice(1, r.size-1).map(_.toDouble)
        LabeledPoint(label,Vectors.dense(features))}
      }
      //2-折叠交叉验证，将原始数据0.9分为训练数据，0.1分为测试数据
      val Array(trainData,cvData)=rawData.randomSplit(Array(0.9,0.1), 123)
    
      val treeModel=DecisionTree.train(trainData, Algo.Classification, Entropy, 29)
      
    
   
    //计算准确率 
    val treeAccuracy=cvData.map { point => {
      if(treeModel.predict(point.features) == point.label) 1 else 0  
    }
    }.sum()/cvData.count()
    println("准确率："+treeAccuracy)
   //
    
  }
  
  
  
  
  
  
  
  
  
  
  
}  