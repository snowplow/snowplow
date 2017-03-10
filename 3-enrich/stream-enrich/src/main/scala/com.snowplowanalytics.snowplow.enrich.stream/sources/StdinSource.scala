 /*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics
package snowplow
package enrich
package stream
package sources

import org.apache.commons.codec.binary.Base64

import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver
import model.EnrichConfig
import scalatracker.Tracker

/** Source to decode raw events (in base64) from stdin. */
class StdinSource(
  config: EnrichConfig,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker]
) extends Source(config, igluResolver, enrichmentRegistry, tracker) {

  override val MaxRecordSize = None

  /** Never-ending processing loop over source stream. */
  override def run(): Unit =
    for (ln <- scala.io.Source.stdin.getLines) {
      val bytes = Base64.decodeBase64(ln)
      enrichAndStoreEvents(List(bytes))
    }
}
