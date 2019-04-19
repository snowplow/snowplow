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
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest._
import Matchers._

import singleton._

class SingletonSpec extends FreeSpec {
  "the singleton object should" - {
    "make a ResolverSingleton.get function available" - {
      "which throws if the resolver can't be parsed" in {
        // resolver is validated at launch time so this can't happen
        a[RuntimeException] should be thrownBy ResolverSingleton.get(JString("a"))
      }
      "which builds and stores the resolver" in {
        ResolverSingleton.get(parse(SpecHelpers.resolverConfig)) shouldEqual SpecHelpers.resolver
      }
      "which retrieves the resolver afterwards" in {
        ResolverSingleton.get(JString("a")) shouldEqual SpecHelpers.resolver
      }
    }
    "make a EnrichmentRegistrySingleton.get function available" - {
      import SpecHelpers.resolver
      "which throws if the registry can't be parsed" in {
        // registry is validated at launch time so this can't happen
        a[RuntimeException] should be thrownBy
          EnrichmentRegistrySingleton.get(JObject(List(("a", JString("a")))))
      }
      "which builds and stores the registry" in {
        val json =
          ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
          ("data" -> List(parse(SpecHelpers.enrichmentConfig)))
        EnrichmentRegistrySingleton.get(json) shouldEqual SpecHelpers.enrichmentRegistry
      }
      "which retrieves the registry afterwards" in {
        EnrichmentRegistrySingleton.get(JObject(List(("a", JString("a"))))) shouldEqual
          SpecHelpers.enrichmentRegistry
      }
    }
  }
}
