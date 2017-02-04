package com.spark.streaming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.spark.streaming.mysql.MysqlManager
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaClusterManager
import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD

object SpartStreamingTest {
  var sc: SparkContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  import org.apache.log4j.{Level,Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val zookeeper="solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  def init() {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set("spark.streaming.mysql.maxRetries", "1")
    sc = new SparkContext(sparkConf)
    
    
  }
  /**
   * 应用场景，（推荐是用时间戳 做 数据 分隔点 的字段）
   * 数据是按时间顺序写入的，所以必须要有个时间的字段，而且这个时间的字段必须是 时间戳，每条数据的时间戳都要不一样
   * 如果数据是同一时间进去的，那必须要把每条数据的时间戳都一直往下加一操作，如：149000000 那同一时间的数据必须是基于这个往下加然后再入mysql
   * 要不然那你就必须是要有一个id（必须也是Long类型，这其实跟自带的那个JdbcRDD差不多）字段，且必须也是一直往下加的，不能往回走，
   * 
   * 
   */
  def main(args: Array[String]): Unit = {
    mySparkInputstream
    
    
  }
  def localSparkStream(){
    init()
     val ssc = new StreamingContext(sc, Seconds(2))
     var topics      = Set("smartadsdeliverylog")
      var kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> "test", "zookeeper.connect" -> zookeeper)
   val dstream= KafkaClusterManager.createDirectStream(ssc, kafkaParams, topics)
    dstream.foreachRDD(rdd=>rdd.foreach{x=>
    val datas=x._2.split(",",-1)
   val price = if(datas(24).trim.nonEmpty) "b" else "w"
    println(price)
    }
        
    )
    ssc.start()
		ssc.awaitTermination()
  }
  def mySparkInputstream{
	  init
	  //查询条件必须是两边都是等号的  ID >= ? AND ID <= ? ，不然会丢数据
	  var sql="SELECT id,name FROM test"
	  val tablename="test"
	  val timeClounm="id"//主键是什么。流式的话，按理应该是时间
	  val fromTime=1//从某个时间点开始
	  val partitionNum=2//分区数
	  val ssc = new StreamingContext(sc, Seconds(2))
//
	  var count=0
	  var r=ssc.createDirectMysqlDStream(getConnection, tablename, timeClounm, 
			                fromTime,sql, partitionNum, sscextractValues)
		r.foreachRDD{x=>println("sssssss");Thread.sleep(2000);println("kkkkkkk");}
		r.foreachRDD{rdd=>
		      count+=1
			    println(count)
		      rdd.foreach(println)
		      
		      
		      if(count<2){
		    	  Thread.sleep(8000)
		      }
	  }
			    
	  //r.printlnDStream("")
 //两个流式一起获取数据
	/*sql="SELECT id,name FROM test where id>10"                                   
	var r2=ssc.createDirectMysqlDStream(getConnection, tablename, rowkeyName, 
                                       fromId,sql, partitionNum, sscextractValues)*/
	/*r2.printlnDStream("r2 :")*/
			  ssc.start()
			  ssc.awaitTermination()
  }
}