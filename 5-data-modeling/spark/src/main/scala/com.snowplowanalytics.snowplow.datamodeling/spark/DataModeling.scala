/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.datamodeling.spark

// Scalaz
import scalaz._
import Scalaz._

// Spark
import org.apache.spark.{
  SparkContext,
  SparkConf
}
import SparkContext._

// Spark SQL
import org.apache.spark.sql.SQLContext

// This project
import events.EventTransformer

object DataModeling {

  private val AppName = "DataModelingJob"

  // Run the data modeling. Agnostic to Spark's current mode of operation: can be run from tests as well as from main
  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {

    val sc = {
      val conf = new SparkConf().setAppName(AppName).setJars(jars)
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    val file = sc.textFile(args(0))

    // Enriched event TSV -> shredded JSON
    val jsons = file
      .map(line => EventTransformer.transform(line))
      .collect { case Success(event) =>
        event
      }

    // Analyze
    val sqlContext = new SQLContext(sc)
    val events = sqlContext.read.json(jsons) // was sqlContext.jsonRDD(jsons)
    val cityCounts = events.groupBy("geo_city").count().rdd

    // TODO: reduce number of output files
    cityCounts.saveAsTextFile(args(1))
  }
}
