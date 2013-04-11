/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive
package test

// Java
import java.util.UUID

// Scala
import scala.collection.immutable.Map

object SnowPlowTest {

	// A grid of tests to run
	type DataGrid = Map[String, SnowPlowEvent]

	// A helper for implicitly checking UUID-format Strings
	// TODO: really we should write a custom Specs2 Matcher,
	// which checks for stringlyTypedUuid
	val stringlyTypedUuid: String => String = s => UUID.fromString(s).toString
}