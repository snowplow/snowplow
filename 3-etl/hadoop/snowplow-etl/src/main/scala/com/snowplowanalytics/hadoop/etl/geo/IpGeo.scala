/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.hadoop.etl.geo

// Hadoop
import org.apache.hadoop

// Scalding
import com.twitter.scalding.Tool

// Caching
// TODO: add the dependency
import com.twitter.util.LruMap

// MaxMind
import com.maxmind.geoip.{Location, LookupService}

class IpGeo(dbFile: String, options: Int = LookupService.GEOIP_MEMORY_CACHE) {

	// Initialise the cache
	val lru = new LruMap[String, Location](10000) // This is of type mutable.Map[String, Location]

	// Start the lookup service
	val maxmind = new LookupService(locationDbFile, options);

}