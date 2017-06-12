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

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import iglu.client.Resolver
import enrich.common.{FatalEtlError, ValidatedMessage, ValidatedNelMessage}

/** Singletons needed for unserializable or stateful classes. */
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
     * @param igluConfig JSON representing the Iglu resolver
     * @return A Resolver or one or more error messages boxed in a Scalaz ValidationNel
     */
    private[spark] def getIgluResolver(igluConfig: String): ValidatedNelMessage[Resolver] =
      for {
        node <- (utils.base64.base64ToJsonNode(igluConfig, "iglu")
          .toValidationNel: ValidatedNelMessage[JsonNode])
        reso <- Resolver.parse(node)
      } yield reso
  }

  /** Singleton for DuplicateStorage to maintain one per node. */
  object DuplicateStorageSingleton {
    @volatile private var instance: Option[DuplicateStorage] = _
    /**
     * Retrieve or build an instance of DuplicateStorage.
     * @param dupStorageConfig configuration for DuplicateStorage
     */
    def get(
      dupStorageConfig: Option[DuplicateStorage.DuplicateStorageConfig]
    ): Option[DuplicateStorage] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = dupStorageConfig.map(DuplicateStorage.initStorage) match {
              case Some(v) => v.fold(e => throw new FatalEtlError(e.toString), c => Some(c))
              case None => None
            }
          }
        }
      }
      instance
    }
  }
}