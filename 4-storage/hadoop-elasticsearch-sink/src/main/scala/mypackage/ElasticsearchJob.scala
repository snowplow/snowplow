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
 * Helpers for our data processing pipeline.
 */
object ElasticsearchJob {

}

/**
 * The Snowplow Shred job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */
class ElasticsearchJob(args : Args) extends Job(args) {

  // TODO: use withJsonInput instead of this Properties object to indicate the data is already JSON
  val props = new java.util.Properties
  props.setProperty("es.input.json", "true")

  val hostArg = args.requiredz("host").toValidationNel
  val resourceArg = args.requiredz("resource").toValidationNel
  val portArg = (for {
    portString <- args.requiredz("port")
    portInt <- try {
      portString.toInt.success
    } catch {
      case nfe: NumberFormatException =>
        s"Couldn't parse port $portString as int: [$nfe]".toProcessingMessage.fail
    }
  } yield portInt).toValidationNel
  val inputArg = args.requiredz("input").toValidationNel

  val result = (hostArg |@| resourceArg |@| portArg |@| inputArg) { (host, resource, port, input) =>
    val writeToES = EsSource(
      resource,
      esHost = Some(host),
      esPort = Some(port),
      settings = Some(props)
      )

    val schema = ('name, 'age, 'address, 'useless)
    val source = MultipleTextLineFiles(input)
      .read
      .mapTo('line -> 'output) {l: String => l}
      .write(writeToES)
  }

  result match {
    case Failure(e) => throw FatalEtlError(e.toString)
    case _ =>
  }

}
