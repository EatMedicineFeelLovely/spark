package com.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import java.util.ArrayList
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.ALS
import org.apache.log4j.Logger
import org.apache.log4j.Level
/**
 * 训练模型其实就是为了选参数
 * 使用一部分已知的数据进行训练
 * 当预测结果的评分和真是数据的均方差较小时或达到要求时，我们就可以保存此模型（参数配置）
 * 对大数据集进行预测评分然后存储在 数据库中
 * 使用时，只要传入user的id，就可以找到预测的评分并排序，得到较高的评分就进行推荐
 * ALS.train表示训练一个ALS模型，model.predict表示使用这个模型进行预测
 */
object ALSDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark Pi")
    System.setProperty("hadoop.home.dir", "f:\\eclipse\\hdplocal2.6.0")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sc = new SparkContext(conf)
    //rank 因子(系数k，矩阵分解是需要 A（X*Y）=U（X*k）L（k*Y） )   numIterations迭代次数
    var rank = 10
    var numIterations = 19
    println(makeModel(sc, rank, numIterations))
    makeRecommend(sc, rank, numIterations)
    /* var resultMSE=new ArrayList[String]
     for(numIterations<- 30 until 31){
    	 val MSE= makeModel(sc,rank,numIterations)
       resultMSE.add(numIterations+":"+MSE)
     }
     println(resultMSE)*/
  }
  def makeRecommend(sc: SparkContext, rank: Int, numIterations: Int) {
    //数据为        用户  item  评分  时间戳
    //取前三个数据
    val data = sc.textFile("file:\\F:\\workspace\\BigData-Test-OtherDemos\\inputFile\\test2.data", sc.defaultMinPartitions)
    val ratings = data.map { _.split(",").take(3) }
      .map { x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble) }
    //训练模型
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    //获得用户和商品的数据集

    //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.
    /**
     * 使用训练出来的模型进行使用，计算+推荐
     */
    val users = data.map { _.split(",").take(3) }.map { x => x(0) }.distinct().collect()
    users.foreach {
      //一次为每个用户推荐
      user =>
        {
          val rs = model.recommendProducts(user.toInt, 10) //参数一为用户，二为返回前几
          var values = ""
          var key = 0
          //拼接推荐结果
          rs.foreach { r =>
            {
              key = r.user
              values = values + r.product + ":" + r.rating + "\n"
            }
          }
          //打印推荐结果
          println(key.toString() + " => " + values)
        }
    }
    //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  }

  def makeModel(sc: SparkContext, rank: Int, numIterations: Int): Double = {
    //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    /**
     * 这一部分是为了训练模型用的
     */

    //数据为        用户  item  评分  时间戳
    //取前三个数据
    val data = sc.textFile("file:\\F:\\workspace\\BigData-Test-OtherDemos\\inputFile\\test2.data", sc.defaultMinPartitions)
    val ratings = data.map { _.split(",") }
      .map { x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble) }
    //训练模型
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    //这里是要生成user product对，每一对都会生成预测，但是如果没有对的话就不生成预测
    //val userProducts=ratings.map { case Rating(user,product,rate) =>  (user,product)}
    val user = sc.textFile("file:\\F:\\workspace\\BigData-Test-OtherDemos\\inputFile\\user", sc.defaultMinPartitions).map { _.toInt }
    val product = sc.textFile("file:\\F:\\workspace\\BigData-Test-OtherDemos\\inputFile\\product", sc.defaultMinPartitions).map { _.toInt }
    //笛卡尔积
    val userProducts = user.cartesian(product)
    //predict使用推荐模型对用户商品进行预测评分，得到预测评分的数据集,这是所有的预测对，
    //前面是user，后面是product，如果没有出现，则不对这一对进行评测
    //如果，有一个product从未出现过，即使你写了对了，那也 不会有预测结果的
    val predictions = model.predict(userProducts).map { case Rating(user, product, rate) => ((user, product), rate) }
    //将真实的评分数据集合预测评分数据集进行合并
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate) }
      .join(predictions)
    //可以清楚地看到，实际评分和预测评分
    ratesAndPreds.foreach(println)
    //然后计算预测的和实际的均方差，均方差越小说明越准确，mean求平均值
    val MSE = ratesAndPreds.map {
      case ((user, products), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    //使用ALS内置的MSE评估
    val predictedAndTrue = ratesAndPreds.map { case ((user, products), (r1, r2)) => (r1, r2) }
    val DefaultMSE = new RegressionMetrics(predictedAndTrue)
    //将模型保存
    //     model.save(sc, "")
    //加载一个model
    //    val loadModel=MatrixFactorizationModel.load(sc, "")

    //打印方差和预测结果
    println("这是预测评分和实际评分的均方差：" + MSE)
    println("这是内置的预测评分和实际评分的均方差MSE：" + DefaultMSE.meanSquaredError)
    //如果均方差满意的话可以将预测的评分进行存储
    val result = predictions.map {
      case ((user, product), rate) => (user, (product, rate))
    }
      .groupByKey
      .map { data =>
        {
          val resultda = data._2.map(product => {
            data._1 + "::" + product._1 + "::" + product._2
          })
          resultda
        }
      }
    result.flatMap(x => x).foreach { println }
    //result.flatMap(x=>x).saveAsTextFile("outfile/ASLresult")
    return MSE
    //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    
  }
}