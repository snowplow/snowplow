/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package storage.spark

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Snowplow
import iglu.client.Resolver
import enrich.common.{FatalEtlError,ValidatedMessage, ValidatedNelMessage}
import enrich.common.utils.{ConversionUtils, JsonUtils}
import iglu.client.validation.ProcessingMessageMethods._

/** Singleton for Iglu's Resolver to maintain one Resolver per node. */
object ResolverSingleton {
  @volatile private var instance: Resolver = _
  /**
   * Retrieve or build an instance of Iglu's Resolver.
   * @param igluConfig JSON representing the Iglu configuration
   */
  def get(igluConfig: String): Resolver = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = getIgluResolver(igluConfig)
            .valueOr(e => throw new FatalEtlError(e.toString))
        }
      }
    }
    instance
  }

  /**
   * Build an Iglu resolver from a JSON.
   * @param json JSON representing the Iglu resolver
   * @return A Resolver or one or more error messages boxed in a Scalaz ValidationNel
   */
  private[spark] def getIgluResolver(json: String): ValidatedNelMessage[Resolver] =
    for {
      node <- (base64ToJsonNode(json, "iglu").toValidationNel: ValidatedNelMessage[JsonNode])
      reso <- Resolver.parse(node)
    } yield reso

  /**
   * Convert a base64-encoded JSON String into a JsonNode.
   * @param str base64-encoded JSON
   * @param field name of the field to be decoded
   * @return a JsonNode on Success, a NonEmptyList of ProcessingMessages on Failure
   */
  private def base64ToJsonNode(str: String, field: String): ValidatedMessage[JsonNode] =
    (for {
      raw  <- ConversionUtils.decodeBase64Url(field, str)
      node <- JsonUtils.extractJson(field, raw)
    } yield node).toProcessingMessage
}
