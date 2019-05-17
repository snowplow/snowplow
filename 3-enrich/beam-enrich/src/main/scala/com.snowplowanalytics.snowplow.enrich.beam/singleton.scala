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
package beam

import org.json4s._

import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver

/** Singletons needed for unserializable classes. */
object singleton {
  /** Singleton for Resolver to maintain one per node. */
  object ResolverSingleton {
    @volatile private var instance: Resolver = _
    /**
     * Retrieve or build an instance of a Resolver.
     * @param resolverJson JSON representing the Resolver
     */
    def get(resolverJson: JValue): Resolver = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = Resolver.parse(resolverJson)
              .valueOr(e => throw new RuntimeException(e.toString))
          }
        }
      }
      instance
    }
  }

  /** Singleton for EnrichmentRegistry. */
  object EnrichmentRegistrySingleton {
    @volatile private var instance: EnrichmentRegistry = _
    /**
     * Retrieve or build an instance of EnrichmentRegistry.
     * @param enrichmentsJson JSON representing the enrichments that need performing
     */
    def get(enrichmentsJson: JObject)(implicit r: Resolver): EnrichmentRegistry = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = EnrichmentRegistry.parse(enrichmentsJson, false)
              .valueOr(e => throw new RuntimeException(e.toString))
          }
        }
      }
      instance
    }
  }
}
