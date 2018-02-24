package org.apache.spark.scheduler 

class MySparkListener extends SparkListener {  
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {  
    println("*************************************************")  
    println("app:end")  
    println("*************************************************")  
  }  
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {  
    println("*************************************************")  
    println("job:end")  
    jobEnd.jobResult match {  
      case JobSucceeded =>  
        println("job:end:JobSucceeded")  
      case JobFailed(exception) =>  
        println("job:end:file")  
        exception.printStackTrace()  
    }  
    println("*************************************************")  
  }  
}