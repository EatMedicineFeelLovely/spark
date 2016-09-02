package com.spark.hive

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.common.HCatUtil
import org.apache.hive.hcatalog.data.schema.HCatSchema
import org.apache.hadoop.mapreduce.Job
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat._
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat
import org.apache.hadoop.io.WritableComparable
import org.apache.spark.SerializableWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.hive.hcatalog.mapreduce.HCatRecordReader
import org.apache.hadoop.mapreduce.JobContext

object SparkRddToHive {
  var sc: SparkContext = null
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    init
    var outputJob: Job = Job.getInstance
    var schema = getHCatSchema("test", "test",outputJob)
    var a = sc.parallelize(Array((1,1)))
    useHCatOutputFormatToHive(outputJob,schema,a)
    println(">>>>>>...")
    //a.saveAsNewAPIHadoopDataset(new Configuration)
  }
  def getHCatSchema(dbName: String, tableName: String,outputJob: Job) = {
    //获取schema
    var schema: HCatSchema = null
    //var outputJob: Job = Job.getInstance
    outputJob.setJobName("getHCatSchema");
    HCatOutputFormat.setOutput(outputJob, OutputJobInfo.create(dbName, tableName, null));
    schema = HCatBaseOutputFormat.getTableSchema(outputJob.getConfiguration());
    HCatOutputFormat.setSchema(outputJob, schema)
    schema
  }
  def useHCatOutputFormatToHive[T:ClassTag](job:Job,recordSchema: HCatSchema,rdd:RDD[T]) {
    var a = sc.parallelize(Array(("test", 1), ("test2", 2), ("test3", 3), ("test4", 4)),2)
    job.setOutputFormatClass(classOf[HCatOutputFormat])
    job.setOutputKeyClass(classOf[NullWritable]);
    job.setOutputValueClass(classOf[DefaultHCatRecord]);
    var jobconf = job.getConfiguration
    
    var c = a.map { x =>
      var record = new DefaultHCatRecord(recordSchema.size());
      record.setString("name", recordSchema, x._1)
      record.setString("age", recordSchema, x._2.toString)
      (NullWritable.get(), record)
    }
    c.saveAsNewAPIHadoopDataset(jobconf)

  }
  def init {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
}