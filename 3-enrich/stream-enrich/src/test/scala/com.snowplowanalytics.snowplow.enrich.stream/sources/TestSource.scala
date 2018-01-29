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

import iglu.client.Resolver
import common.enrichments.EnrichmentRegistry
import model.EnrichConfig
import scalatracker.Tracker

/**
 * Source to allow the testing framework to enrich events
 * using the same methods from AbstractSource as the other
 * sources.
 */
class TestSource(
  config: EnrichConfig,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker]
) extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {

  override val outputAsJson = false

  override def run(): Unit =
    throw new RuntimeException("run() should not be called on TestSource")
}
