package com.snowplowanalytics.snowplow.enrich.spark

import org.apache.spark._
import org.apache.spark.sql._


trait RunnableSparkJob {

  def run(spark: SparkSession): Unit

  def sparkConfig(): SparkConf

  final def main(args: Array[String]): Unit = {
    val config = sparkConfig()
    val spark = SparkSession.builder()
      .config(config)
      .getOrCreate()
    run(spark)
  }

}
