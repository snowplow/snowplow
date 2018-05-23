/*
 * Copyright (c) 2017-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object Mutators {

  /**
   * This and the next constant maps from a configuration field name to an EnrichedEvent mutator. The structure is such so that
   * it preserves type safety, and it can be easily replaced in the future by generated code that will use the configuration as
   * input.
   */
  val ScalarMutators: Map[String, Mutator] = Map(
    "user_id" -> Mutator(
      "user_id", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.user_id, strategy)
        event.user_id = newValue
        modifiedFields
      }
    ),
    "user_ipaddress" -> Mutator(
      "user_ipaddress", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.user_ipaddress, strategy)
        event.user_ipaddress = newValue
        modifiedFields
      }
    ),
    "user_fingerprint" -> Mutator(
      "user_fingerprint", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.user_fingerprint, strategy)
        event.user_fingerprint = newValue
        modifiedFields
      }
    ),
    "domain_userid" -> Mutator(
      "domain_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.domain_userid, strategy)
        event.domain_userid = newValue
        modifiedFields
      }
    ),
    "network_userid" -> Mutator(
      "network_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.network_userid, strategy)
        event.network_userid = newValue
        modifiedFields
      }
    ),
    "ip_organization" -> Mutator(
      "ip_organization", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.ip_organization, strategy)
        event.ip_organization = newValue
        modifiedFields
      }
    ),
    "ip_domain" -> Mutator(
      "ip_domain", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.ip_domain, strategy)
        event.ip_domain = newValue
        modifiedFields
      }
    ),
    "tr_orderid" -> Mutator(
      "tr_orderid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.tr_orderid, strategy)
        event.tr_orderid = newValue
        modifiedFields
      }
    ),
    "ti_orderid" -> Mutator(
      "ti_orderid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.ti_orderid, strategy)
        event.ti_orderid = newValue
        modifiedFields
      }
    ),
    "mkt_term" -> Mutator(
      "mkt_term", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_term, strategy)
        event.mkt_term = newValue
        modifiedFields
      }
    ),
    "mkt_content" -> Mutator(
      "mkt_content", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_content, strategy)
        event.mkt_content = newValue
        modifiedFields
      }
    ),
    "se_category" -> Mutator(
      "se_category", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.se_category, strategy)
        event.se_category = newValue
        modifiedFields
      }
    ),
    "se_action" -> Mutator(
      "se_action", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.se_action, strategy)
        event.se_action = newValue
        modifiedFields
      }
    ),
    "se_label" -> Mutator(
      "se_label", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.se_label, strategy)
        event.se_label = newValue
        modifiedFields
      }
    ),
    "se_property" -> Mutator(
      "se_property", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.se_property, strategy)
        event.se_property = newValue
        modifiedFields
      }
    ),
    "mkt_clickid" -> Mutator(
      "mkt_clickid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_clickid, strategy)
        event.mkt_clickid = newValue
        modifiedFields
      }
    ),
    "refr_domain_userid" -> Mutator(
      "refr_domain_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.refr_domain_userid, strategy)
        event.refr_domain_userid = newValue
        modifiedFields
      }
    ),
    "domain_sessionid" -> Mutator(
      "domain_sessionid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.domain_sessionid, strategy)
        event.domain_sessionid = newValue
        modifiedFields
      }
    )
  )

  val JsonMutators: Map[String, Mutator] = Map(
    "contexts" -> Mutator(
      "contexts", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.contexts, strategy)
        event.contexts = newValue
        modifiedFields
      }
    ),
    "derived_contexts" -> Mutator(
      "derived_contexts", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.derived_contexts, strategy)
        event.derived_contexts = newValue
        modifiedFields
      }
    ),
    "unstruct_event" -> Mutator(
      "unstruct_event", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStrategyFn) =>
        val (newValue, modifiedFields) = fn(event.unstruct_event, strategy)
        event.unstruct_event = newValue
        modifiedFields
      }
    )
  )
}
