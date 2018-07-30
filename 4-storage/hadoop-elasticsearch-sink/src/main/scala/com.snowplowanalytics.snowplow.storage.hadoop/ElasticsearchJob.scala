/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package storage
package hadoop

// Cascading
import cascading.tap.SinkMode
import cascading.tuple.Fields

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._
import io.scalding.taps.elasticsearch.EsSource

// Common Enrich
import enrich.common.utils.ScalazArgs._
import enrich.common.FatalEtlError

// Iglu
import iglu.client.validation.ProcessingMessageMethods._

/**
 * The Snowplow Elasticsearch Sink job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */
class ElasticsearchJob(args : Args) extends Job(args) {

  ElasticsearchJobConfig.fromScaldingArgs(args).fold(
    err => throw FatalEtlError(err.toString),
    configuration => runJob(configuration)
  )

  /**
   * Create the required pipes and run the job
   *
   * @param configuration
   */
  def runJob(configuration: ElasticsearchJobConfig) {

    // Optional sleep to give S3 time to become consistent
    configuration.delaySeconds.foreach(t => Thread.sleep(t * 1000))

    val esSink = configuration.getEsSink
    val inputPipe = MultipleTextLineFiles(configuration.input).read
    val trappableInput = configuration.exceptionsFolder match {
      case None => inputPipe
      case Some(folder) => inputPipe.addTrap(Tsv(folder))
    }

    trappableInput
      .mapTo('line -> 'output) {l: String => l}
      .write(esSink)
  }

}
