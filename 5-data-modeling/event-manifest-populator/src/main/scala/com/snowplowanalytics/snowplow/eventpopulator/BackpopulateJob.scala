/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.eventpopulator

// Spark
import org.apache.spark.{SparkConf, SparkContext}

// This project
import Main.JobConf
import Utils._

object BackpopulateJob {

  /**
    * Store event's duplication triple in DynamoDB
    * Can throw exception, that could short-circuit whole job
    */
  def store(triple: DeduplicationTriple, storage: DuplicateStorage): Unit = {
    val _ = storage.put(eventId = triple.eventId, eventFingerprint = triple.fingerprint, etlTstamp = triple.etlTstamp)
  }

  def run(jobConfig: JobConf) = {

    lazy val storage = DuplicateStorage.initStorage(jobConfig.storageConfig).fold(
      e => throw new RuntimeException(e.toString),
      r => r)

    val config = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(config)

    jobConfig.since match {
      case Some(_) => runInSplits()
      case None => runInBatch()
    }

    /**
      * Run job for enrichment archives since `jobConfig.since`
      * It'll sequentially process each directory in separate RDDs.
      * It splits jobs into separate RDDs because there's no way load
      * data only for particular subfolders in a single RDD
      */
    def runInSplits(): Unit = {
      val runs = getRuns(jobConfig)
      runs.foreach { runId =>
        println(s"Processing $runId")
        val events = sc.textFile(s"s3a://${jobConfig.enrichedInBucket}run=$runId/part-*")
        events.map(lineToTriple).foreach { triple => store(triple, storage) }
      }
    }

    /**
      * Process whole archive in a single batch (single RDD)
      */
    def runInBatch(): Unit = {
      val events = sc.textFile(s"s3a://${jobConfig.enrichedInBucket}run=*/part-*")
      events.map(lineToTriple).foreach { triple => store(triple, storage) }
    }
  }
}
