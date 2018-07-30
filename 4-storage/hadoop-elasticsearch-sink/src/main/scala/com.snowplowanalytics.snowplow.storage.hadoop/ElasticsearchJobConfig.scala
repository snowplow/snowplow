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

// Java
import java.util.Properties

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._
import io.scalding.taps.elasticsearch.EsSource

// Common Enrich
import enrich.common.utils.ScalazArgs._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._

object ElasticsearchJobConfig {

  /**
   * Validate and parse the Scalding arguments
   *
   * @param args
   * @return Validated ElasticsearchJobConfig
   */
  def fromScaldingArgs(args: Args) = {

    // TODO: use withJsonInput instead of this Properties object to indicate the data is already JSON
    val esProperties = new Properties
    esProperties.setProperty("es.input.json", "true")

    val hostArg = args.requiredz("host").toValidationNel
    val resourceArg =
      (args.requiredz("index").toValidationNel |@| args.requiredz("type").toValidationNel)(_ + "/" + _)
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
    val exceptionsFolder = args.optionalz("exceptions_folder").toValidationNel

    val delaySeconds = (for {
      delayString <- args.optionalz("delay")
      delayInt <- try {
        delayString.map(_.toLong).success
      } catch  {
        case nfe: NumberFormatException =>
          s"Couldn't parse delay $delayString as int: [$nfe]".toProcessingMessage.fail
      }
    } yield delayInt).toValidationNel

    val validatedEsProperties = (args.optionalz("es_nodes_wan_only") flatMap {
      case Some("true") =>
        esProperties.setProperty("es.nodes.wan.only", "true")
        esProperties.success
      case None | Some("false") =>
        esProperties.success
      case Some(other) => s"es_nodes_wan_only must be true or false, not $other".toProcessingMessage.fail
    }).toValidationNel

    (
      hostArg |@|
      resourceArg |@|
      portArg |@|
      inputArg |@|
      validatedEsProperties |@|
      exceptionsFolder |@|
      delaySeconds
    )(ElasticsearchJobConfig(_,_,_,_,_,_,_))
  }
}

/**
 * Configuration for the Scalding job
 *
 * @param host Host for the Elasticsearch cluster
 * @param resource Elasticsearch path of the form ${index}/${type}
 * @param input Source directory
 * @param settings Additional configuration for the EsSource
 * @param exceptionsFolder Folder where exceptions are stored
 * @param delaySeconds How long to wait for S3 consistency before starting the job
 */
case class ElasticsearchJobConfig(
  host: String,
  resource: String,
  port: Int,
  input: String,
  settings: Properties,
  exceptionsFolder: Option[String] = None,
  delaySeconds: Option[Long] = None
  ) {

  def getEsSink = EsSource(resource, esHost = host.some, esPort = port.some, settings = settings.some)
}
