package com.snowplowanalytics.snowplow.enrich.spark

import org.apache.spark._
import org.apache.spark.sql._

object EtlJob extends RunnableSparkJob {

  override def sparkConfig(): SparkConf = {
    val config = new SparkConf()
    config.setAppName(classOf[EtlJob].toString)

    config.setIfMissing("spark.master", "local[*]")
  }

  override def run(spark: SparkSession): Unit = {
    val job = EtlJob(spark)
    job.run()
  }

  def apply(spark: SparkSession) = new EtlJob(spark)

}

class EtlJob(@transient val spark: SparkSession) {

  @transient val sc = spark.sparkContext
  @transient val sqlContext = spark.sqlContext

  def run(): Unit = {
    // Etl Job logic here ...
    val c = sc.makeRDD(Seq(1,2,3,4)).count()
    println(c)
  }
}
