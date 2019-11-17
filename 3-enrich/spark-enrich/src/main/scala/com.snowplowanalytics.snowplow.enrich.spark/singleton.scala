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

// cats
import cats.Id
import cats.data.NonEmptyList
import cats.syntax.either._

// circe
import io.circe.Json

// Snowplow
import common.FatalEtlError
import common.enrichments.EnrichmentRegistry

// Iglu
import iglu.client.Client

/** Singletons needed for unserializable classes. */
object singleton {

  /** Singleton for Iglu's Resolver to maintain one Resolver per node. */
  object ResolverSingleton {
    @volatile private var instance: Client[Id, Json] = _

    /**
     * Retrieve or build an instance of Iglu's Resolver.
     * @param igluConfig JSON representing the Iglu configuration
     */
    def get(igluConfig: Json): Client[Id, Json] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = Client
              .parseDefault[Id](igluConfig)
              .valueOr(e => throw new FatalEtlError(e.toString))
          }
        }
      }
      instance
    }
  }

  /** Singleton for EnrichmentRegistry. */
  object RegistrySingleton {
    @volatile private var instance: EnrichmentRegistry[Id] = _
    @volatile private var enrichments: Json                = _

    /**
     * Retrieve or build an instance of EnrichmentRegistry.
     * @param igluConfig JSON representing the Iglu configuration
     * @param enrichments JSON representing the enrichments that need performing
     * @param local Whether to build a registry from local data
     */
    def get(igluConfig: Json, enrichments: Json, local: Boolean): EnrichmentRegistry[Id] = {
      if (instance == null || this.enrichments != enrichments) {
        synchronized {
          if (instance == null || this.enrichments != enrichments) {
            val client = ResolverSingleton.get(igluConfig)
            val registry =
              EnrichmentRegistry.parse[Id](enrichments, client, local).toEither.toEitherT[Id]
            instance = registry
              .flatMap(x => EnrichmentRegistry.build[Id](x).leftMap(NonEmptyList.one))
              .valueOr(e => throw new FatalEtlError(e.toList.mkString("\n")))
            this.enrichments = enrichments
          }
        }
      }
      instance
    }

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
}
