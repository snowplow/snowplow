/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry
package snowplow

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.either._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe._
import io.circe.syntax._

import loaders.CollectorPayload
import utils.{JsonUtils => JU}
import utils.{ConversionUtils => CU}

/**
 * The Redirect Adapter is essentially a pre-processor for
 * Snowplow Tracker Protocol v2 above (although it doesn't
 * use the TP2 code above directly).
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
    val UriRedirect =
      SchemaKey("com.snowplowanalytics.snowplow", "uri_redirect", "jsonschema", "1-0-0").toSchemaUri
  }

  /**
   * Converts a CollectorPayload instance into raw events. Assumes we have a GET querystring with
   * a u parameter for the URI redirect and other parameters per the Snowplow Tracker Protocol.
   * @param payload The CollectorPaylod containing one or more raw events as collected by a
   * Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(
    implicit resolver: Resolver
  ): ValidatedNel[String, NonEmptyList[RawEvent]] = {
    val originalParams = toMap(payload.querystring)
    if (originalParams.isEmpty) {
      "Querystring is empty: cannot be a valid URI redirect".invalidNel
    } else {
      originalParams.get("u") match {
        case None => "Querystring does not contain u parameter: not a valid URI redirect".invalidNel
        case Some(u) =>
          val json = buildUriRedirect(u)
          val newParams: Either[String, Map[String, String]] =
            if (originalParams.contains("e")) {
              // Already have an event so add the URI redirect as a context (more fiddly)
              def newCo = Map("co" -> toContext(json).noSpaces)
              (originalParams.get("cx"), originalParams.get("co")) match {
                case (None, None) => newCo.asRight
                case (None, Some(co)) if co == "" => newCo.asRight
                case (None, Some(co)) => addToExistingCo(json, co).map(str => Map("co" -> str))
                case (Some(cx), _) => addToExistingCx(json, cx).map(str => Map("cx" -> str))
              }
            } else {
              // Add URI redirect as an unstructured event
              Map("e" -> "ue", "ue_pr" -> toUnstructEvent(json).noSpaces).asRight
            }

          val fixedParams = Map(
            "tv" -> TrackerVersion,
            "p" -> originalParams.getOrElse("p", TrackerPlatform) // Required field
          )

          (for {
            np <- newParams
            ev = NonEmptyList.one(
              RawEvent(
                api = payload.api,
                parameters = (originalParams - "u") ++ np ++ fixedParams,
                contentType = payload.contentType,
                source = payload.source,
                context = payload.context
              ))
          } yield ev).leftMap(e => NonEmptyList.one(e)).toValidated
      }
    }
  }

  /**
   * Builds a self-describing JSON representing a URI redirect entity.
   * @param uri The URI we are redirecting to
   * @return a URI redirect as a self-describing JValue
   */
  private def buildUriRedirect(uri: String): Json =
    Json.obj(
      "schema" := SchemaUris.UriRedirect,
      "data" := Json.obj("uri" := uri)
    )

  /**
   * Adds a context to an existing non-Base64-encoded self-describing contexts stringified JSON.
   * Does the minimal amount of validation required to ensure the context can be safely added, or
   * returns a Failure.
   * @param new The context to add to the existing list of contexts
   * @param existing The existing contexts as a non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-describing contexts stringified JSON
   */
  private def addToExistingCo(newContext: Json, existing: String): Either[String, String] =
    for {
      json <- JU.extractJson("co|cx", existing)
      merged = json.hcursor
        .downField("data")
        .withFocus(_.mapArray(newContext +: _))
        .top
        .getOrElse(json)
    } yield merged.noSpaces

  /**
   * Adds a context to an existing Base64-encoded self-describing contexts stringified JSON.
   * Does the minimal amount of validation required to ensure the context can be safely added, or
   * returns a Failure.
   * @param new The context to add to the existing list of contexts
   * @param existing The existing contexts as a non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-describing contexts stringified JSON
   */
  private def addToExistingCx(newContext: Json, existing: String): Either[String, String] =
    for {
      decoded <- CU.decodeBase64Url("cx", existing)
      added <- addToExistingCo(newContext, decoded)
      recoded = CU.encodeBase64Url(added)
    } yield recoded

}
