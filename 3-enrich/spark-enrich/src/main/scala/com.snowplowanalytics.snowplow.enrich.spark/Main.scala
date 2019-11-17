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

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import com.snowplowanalytics.snowplow.enrich.spark.EnrichJob.classesToRegister

object Main {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master", "local[*]")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(classesToRegister)

    val spark = SparkSession
      .builder()
      .config(config)
      .getOrCreate()
    EnrichJob(spark, args).run()
    spark.stop()
  }
}
