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

/**
 * Helper object to simplify calling the
 * SnowPlowEventDeserializer from Scala
 * for testing purposes.
 */
object SnowPlowDeserializer {

	/**
	 * Deserialize a line using the SnowPlowEventDeserializer.
	 * Don't make the output conform to a SnowPlowEventStruct.
	 */
	def deserializeUntyped(line: String, continueOn: Boolean = false)(implicit debug: Boolean) = {
		SnowPlowEventDeserializer.deserializeLine(line, debug, continueOn)
	}

	/**
	 * Deserialize a line using the SnowPlowEventDeserializer.
	 * Uses deserializeUntyped().
	 * Make the output conform to a SnowPlowEventStruct.
	 */
	def deserialize(line: String, continueOn: Boolean = false)(implicit debug: Boolean): SnowPlowEventStruct = {
		deserializeUntyped(line, continueOn)(debug).asInstanceOf[SnowPlowEventStruct]
	}
}