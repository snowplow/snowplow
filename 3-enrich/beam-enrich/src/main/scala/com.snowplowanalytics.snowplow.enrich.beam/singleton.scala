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
package com.snowplowanalytics.snowplow.enrich.beam

import cats.Id
import cats.syntax.either._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import io.circe.Json

/** Singletons needed for unserializable classes. */
object singleton {
  /** Singleton for Resolver to maintain one per node. */
  object ClientSingleton {
    @volatile private var instance: Client[Id, Json] = _
    /**
     * Retrieve or build an instance of a Resolver.
     * @param resolverJson JSON representing the Resolver
     */
    def get(resolverJson: Json): Client[Id, Json] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = Client.parseDefault[Id](resolverJson)
              .valueOr(e => throw new RuntimeException(e.toString))
          }
        }
      }
      instance
    }
  }

  /** Singleton for EnrichmentRegistry. */
  object EnrichmentRegistrySingleton {
    @volatile private var instance: EnrichmentRegistry[Id] = _
    /**
     * Retrieve or build an instance of EnrichmentRegistry.
     * @param enrichmentConfs list of enabled enrichment configuration
     * @param client iglu client
     */
    def get(
      enrichmentConfs: List[EnrichmentConf],
      client: Client[Id, Json]
    ): EnrichmentRegistry[Id] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = EnrichmentRegistry.build[Id](enrichmentConfs).value
              .valueOr(e => throw new RuntimeException(e.toString))
          }
        }
      }
      instance
    }
  }
}
