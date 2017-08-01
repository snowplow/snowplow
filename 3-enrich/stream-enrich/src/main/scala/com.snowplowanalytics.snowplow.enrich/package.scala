/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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

/**
 * Scala package object to hold types,
 * helper methods etc.
 *
 * See:
 * http://www.artima.com/scalazine/articles/package_objects.html
 */
package object stream {

  /**
   * Kinesis records must not exceed 1MB
   */
  val MaxBytes = 1000000L

  /** 
   * The enrichment process takes input SnowplowRawEvent objects from
   * an input source and outputs enriched objects to a sink,
   * as defined in the following enumerations.
   */
  object Source extends Enumeration {
    type Source = Value
    val Kafka, Kinesis, Stdin, Test = Value
  }

  object Sink extends Enumeration {
    type Sink = Value
    val Kafka, Kinesis, Stdouterr, Test = Value
  }
}
