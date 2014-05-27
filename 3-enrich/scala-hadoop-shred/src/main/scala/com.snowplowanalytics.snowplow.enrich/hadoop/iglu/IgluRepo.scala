/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package iglu

// Jackson
import com.github.fge.jackson.JsonLoader

/**
 * Provides access to the Iglu repository.
 */
object IgluRepo {
  
  object Schemas {
    private val path = "/jsonschema/com.snowplowanalytics.snowplow"
    val Contexts = JsonLoader.fromResource(s"$path/contexts.json")
    val UnstructEvent = JsonLoader.fromResource(s"$path/unstruct_event.json")
  }

  object SelfDesc {
    private val path = "/jsonschema/com.snowplowanalytics.self-desc"
    val Schema = JsonLoader.fromResource(s"$path/iglu-instance.json")
  }
}
