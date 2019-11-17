/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import org.specs2.specification.BeforeAfterAll

/**
 * Trait to mix in in every spec which has to run a Spark job.
 * Create a spark session before the spec and delete it afterwards.
 */
trait SparkSpec extends BeforeAfterAll {
  def appName: String

  // local[1] means the tests will run locally on one thread
  val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName(appName)
    .set("spark.serializer", classOf[KryoSerializer].getName())
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(EnrichJob.classesToRegister)
  var spark: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  val hadoopConfig = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("io.compression.codecs", classOf[com.hadoop.compression.lzo.LzopCodec].getName())
  hadoopConfig.set(
    "io.compression.codec.lzo.class",
    classOf[com.hadoop.compression.lzo.LzoCodec].getName())

  override def beforeAll(): Unit =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    spark = null
  }
}
