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
package com.snowplowanalytics
package snowplow.enrich
package spark

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Json4s
import org.json4s.jackson.JsonMethods.fromJsonNode

// Snowplow
import common.{FatalEtlError, ValidatedMessage, ValidatedNelMessage}
import common.enrichments.EnrichmentRegistry
import common.utils.{ConversionUtils, JsonUtils}

// Iglu
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

/** Singletons needed for unserializable classes. */
object singleton {

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
        node <- base64ToJsonNode(json, "iglu").toValidationNel: ValidatedNelMessage[JsonNode]
        reso <- Resolver.parse(node)
      } yield reso
  }

  /** Singleton for EnrichmentRegistry. */
  object RegistrySingleton {
    @volatile private var instance: EnrichmentRegistry = _
    @volatile private var enrichments: String          = _

    /**
     * Retrieve or build an instance of EnrichmentRegistry.
     * @param igluConfig JSON representing the Iglu configuration
     * @param enrichments JSON representing the enrichments that need performing
     * @param local Whether to build a registry from local data
     */
    def get(igluConfig: String, enrichments: String, local: Boolean): EnrichmentRegistry = {
      if (instance == null || this.enrichments != enrichments) {
        synchronized {
          if (instance == null || this.enrichments != enrichments) {
            implicit val resolver = ResolverSingleton.get(igluConfig)
            instance = getEnrichmentRegistry(enrichments, local)
              .valueOr(e => throw new FatalEtlError(e.toString))
            this.enrichments = enrichments
          }
        }
      }
      instance
    }

    /**
     * Build an EnrichmentRegistry from the enrichments arg.
     * @param enrichments The JSON of all enrichments constructed by EmrEtlRunner
     * @param local Whether to build a registry from local data
     * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
     * @return An EnrichmentRegistry or one or more error messages boxed in a Scalaz ValidationNel
     */
    private[spark] def getEnrichmentRegistry(enrichments: String, local: Boolean)(
      implicit resolver: Resolver): ValidatedNelMessage[EnrichmentRegistry] =
      for {
        node <- base64ToJsonNode(enrichments, "enrichments").toValidationNel: ValidatedNelMessage[
          JsonNode]
        reg <- EnrichmentRegistry.parse(fromJsonNode(node), local)
      } yield reg
  }

  /** Singleton for Loader. */
  object LoaderSingleton {
    import common.loaders.Loader
    @volatile private var instance: Loader[_] = _
    @volatile private var inFormat: String    = _

    /**
     * Retrieve or build an instance of EnrichmentRegistry.
     * @param inFormat Collector format in which the data is coming in
     */
    def get(inFormat: String): Loader[_] = {
      if (instance == null || this.inFormat != inFormat) {
        synchronized {
          if (instance == null || this.inFormat != inFormat) {
            instance = Loader
              .getLoader(inFormat)
              .valueOr(e => throw new FatalEtlError(e.toString))
            this.inFormat = inFormat
          }
        }
      }
      instance
    }
  }

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
