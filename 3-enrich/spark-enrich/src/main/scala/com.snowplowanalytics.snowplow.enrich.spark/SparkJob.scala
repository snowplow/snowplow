/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Utility trait to mix in when writing a Spark job. */
trait SparkJob {
  def run(spark: SparkSession, args: Array[String]): Unit

  def sparkConfig(): SparkConf

  def main(args: Array[String]): Unit = {
    val config = sparkConfig()
    val spark = SparkSession.builder()
      .config(config)
      .getOrCreate()
    run(spark, args)
    spark.stop()
  }
}
