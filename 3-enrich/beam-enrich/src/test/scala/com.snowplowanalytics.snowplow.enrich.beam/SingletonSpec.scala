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

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry._
import io.circe.literal._
import org.scalatest._
import org.scalatest.Matchers._

import singleton._

class SingletonSpec extends FreeSpec {
  "the singleton object should" - {
    "make a ClientSingleton.get function available" - {
      "which throws if the resolver can't be parsed" in {
        // resolver is validated at launch time so this can't happen
        a[RuntimeException] should be thrownBy ClientSingleton.get(json"""{}""")
      }
      "which builds and stores the resolver" in {
        ClientSingleton.get(SpecHelpers.resolverConfig).resolver.repos shouldEqual
          SpecHelpers.client.resolver.repos
      }
      "which retrieves the resolver afterwards" in {
        ClientSingleton.get(json"""{}""").resolver.repos shouldEqual
          SpecHelpers.client.resolver.repos
      }
    }
    "make a EnrichmentRegistrySingleton.get function available" - {
      "which builds and stores the registry" in {
        val reg =
          EnrichmentRegistrySingleton.get(List(AnonIpConf(AnonOctets.Two)), SpecHelpers.client)
        reg.anonIp shouldBe defined
      }
      "which retrieves the registry afterwards" in {
        val reg = EnrichmentRegistrySingleton.get(Nil, SpecHelpers.client)
        reg.anonIp shouldBe defined
      }
    }
  }
}
