/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package enrich
package common
package adapters
package registry

import com.fasterxml.jackson.core.JsonParseException
import iglu.client.Resolver
import common.loaders.CollectorPayload
import common.utils.HttpClient
import org.json4s.JsonAST.{JNothing, JNull}
import org.json4s.JsonDSL._
import org.json4s.MappingException
import org.json4s.jackson.JsonMethods._
import scalaz.Scalaz._
import scalaz.{Failure, Success, Validation}

import scala.util.control.NonFatal

/**
 * An adapter for an enrichment that is handled by a remote webservice.
 *
 * @constructor create a new client to talk to the given remote webservice.
 * @param remoteUrl the url of the remote webservice, e.g. http://localhost/myEnrichment
 * @param connectionTimeout max duration of each connection attempt
 * @param readTimeout max duration of read wait time
 */
class RemoteAdapter(
  val remoteUrl: String,
  val connectionTimeout: Option[Long],
  val readTimeout: Option[Long])
    extends Adapter {

  val bodyMissingErrorText = "Missing payload body"
  val missingEventsErrorText = "Missing events in the response"
  val emptyResponseErrorText = "Empty response"
  val incompatibleResponseErrorText = "Incompatible response, missing error and events fields"

  /**
   * POST the given payload to the remote webservice,
   * wait for it to respond with an Either[List[String], List[RawEvent] ],
   * and return that as a ValidatedRawEvents
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    payload.body match {
      case Some(body) if body.nonEmpty =>
        val json = ("contentType" -> payload.contentType) ~
          ("queryString" -> toMap(payload.querystring)) ~
          ("headers" -> payload.context.headers) ~
          ("body" -> payload.body)
        val request = HttpClient.buildRequest(
          remoteUrl,
          authUser = None,
          authPassword = None,
          Some(compact(render(json))),
          "POST",
          connectionTimeout,
          readTimeout)
        processResponse(payload, HttpClient.getBody(request))

      case _ => bodyMissingErrorText.failNel
    }

  def processResponse(payload: CollectorPayload, response: Validation[Throwable, String]) =
    response match {
      case Failure(throwable) =>
        throwable.getMessage.failNel
      case Success(bodyAsString) =>
        try {
          if (bodyAsString == "") {
            emptyResponseErrorText.failNel
          } else {
            (parse(bodyAsString) \ "error", parse(bodyAsString) \ "events") match {
              case (JNull, JNull) | (JNothing, JNothing) => incompatibleResponseErrorText.failNel
              case (error, JNull | JNothing) => error.extract[String].failNel
              case (JNull | JNothing, eventsObj) =>
                val events = eventsObj.extract[List[Map[String, String]]]
                rawEventsListProcessor(events.map { event =>
                  RawEvent(
                    api = payload.api,
                    parameters = event,
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  ).success
                })
              case _ => s"Unable to parse response: ${bodyAsString}".failNel
            }
          }

        } catch {
          case e: MappingException =>
            s"The events field should be List[Map[String, String]], error: ${e} - response: ${bodyAsString}".failNel
          case e: JsonParseException =>
            s"Json is not parsable, error: ${e} - response: ${bodyAsString}".failNel
          case NonFatal(e) => s"Unexpected error: $e".failNel
        }
    }
}
