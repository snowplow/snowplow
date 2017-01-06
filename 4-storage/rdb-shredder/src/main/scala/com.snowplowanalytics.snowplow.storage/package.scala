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
package storage

// Iglu Scala Client
import iglu.client.JsonSchemaPair

/**
 * Scala package object to hold types, helper methods etc.
 * See: http://www.artima.com/scalazine/articles/package_objects.html
 */
package object spark {
  /** Convenient for passing around the parts of an event. */
  type EventComponents = Tuple4[String, String, List[JsonSchemaPair], String]
}
