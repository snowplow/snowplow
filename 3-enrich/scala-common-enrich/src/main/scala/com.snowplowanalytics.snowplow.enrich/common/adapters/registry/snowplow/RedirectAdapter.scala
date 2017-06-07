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
package enrich
package common
package adapters
package registry
package snowplow

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Iglu
import iglu.client.{
  Resolver,
  SchemaKey
}
import iglu.client.validation.ValidatableJsonMethods._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}
import utils.{ConversionUtils => CU}

/**
 * The Redirect Adapter is essentially a pre-processor for
 * Snowplow Tracker Protocol v2 above (although it doesn't
 * use the TP2 code above directly).
 *
 * The &u= parameter used for a redirect is converted into
 * a URI Redirect entity and then either stored as an
 * unstructured event, added to an existing contexts array
 * or used to initialize a new contexts array.
 */
object RedirectAdapter extends Adapter {

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "r-tp2"

  // Our default tracker platform
  private val TrackerPlatform = "web"

  // Schema for a URI redirect. Could end up being an event or a context
  // depending on what else is in the payload
  private object SchemaUris {
    val UriRedirect = SchemaKey("com.snowplowanalytics.snowplow", "uri_redirect", "jsonschema", "1-0-0").toSchemaUri
  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * Assumes we have a GET querystring with a u parameter
   * for the URI redirect and other parameters per the
   * Snowplow Tracker Protocol.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {

    val originalParams = toMap(payload.querystring)
    if (originalParams.isEmpty) {
      "Querystring is empty: cannot be a valid URI redirect".failNel
    } else {
      originalParams.get("u") match {
        case None    => "Querystring does not contain u parameter: not a valid URI redirect".failNel
        case Some(u) =>

          val json = buildUriRedirect(u)
          val newParams =
            if (originalParams.contains("e")) {
              // Already have an event so add the URI redirect as a context (more fiddly)
              def newCo = Map("co" -> compact(toContext(json))).successNel
              (originalParams.get("cx"), originalParams.get("co")) match {
                case (None, None)                 => newCo
                case (None, Some(co)) if co == "" => newCo
                case (None, Some(co))             => addToExistingCo(json, co)
                                                       .map(str => Map("co" -> str))
                case (Some(cx), _)                => addToExistingCx(json, cx)
                                                       .map(str => Map("cx" -> str))
              }
            } else {
              // Add URI redirect as an unstructured event 
              Map("e" -> "ue", "ue_pr" -> compact(toUnstructEvent(json))).successNel
            }

          val fixedParams = Map(
            "tv" -> TrackerVersion,
            "p"  -> originalParams.getOrElse("p", TrackerPlatform) // Required field
            )

          for {
            np <- newParams
            ev = NonEmptyList(RawEvent(
              api          = payload.api,
              parameters   = (originalParams - "u") ++ np ++ fixedParams,
              contentType  = payload.contentType,
              source       = payload.source,
              context      = payload.context
              ))
          } yield ev
      }
    }
  }

  /**
   * Builds a self-describing JSON representing a
   * URI redirect entity.
   *
   * @param uri The URI we are redirecting to
   * @return a URI redirect as a self-describing
   *         JValue
   */
  private def buildUriRedirect(uri: String): JValue =
    ("schema" -> SchemaUris.UriRedirect) ~
    ("data" -> (
      ("uri" -> uri)
    ))

  /**
   * Adds a context to an existing non-Base64-encoded
   * self-describing contexts stringified JSON.
   *
   * Does the minimal amount of validation required
   * to ensure the context can be safely added, or
   * returns a Failure.
   *
   * @param new The context to add to the
   *        existing list of contexts
   * @param existing The existing contexts as a
   *        non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-
   *         describing contexts stringified JSON
   */
  private def addToExistingCo(newContext: JValue, existing: String): Validated[String] =
    for {
      node   <- JU.extractJson("co|cx", existing).toValidationNel: Validated[JsonNode]
      jvalue  = fromJsonNode(node)
      merged  = jvalue merge render("data" -> List(newContext))
    } yield compact(merged)

  /**
   * Adds a context to an existing Base64-encoded
   * self-describing contexts stringified JSON.
   *
   * Does the minimal amount of validation required
   * to ensure the context can be safely added, or
   * returns a Failure.
   *
   * @param new The context to add to the
   *        existing list of contexts
   * @param existing The existing contexts as a
   *        non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-
   *         describing contexts stringified JSON
   */
  private def addToExistingCx(newContext: JValue, existing: String): Validated[String] =
    for {
      decoded <- CU.decodeBase64Url("cx", existing).toValidationNel: Validated[String]
      added   <- addToExistingCo(newContext, decoded)
      recoded  = CU.encodeBase64Url(added)
    } yield recoded

}
