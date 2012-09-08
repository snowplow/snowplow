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

// LRU
import com.twitter.util.LruMap

// MaxMind
import com.maxmind.geoip.{Location, LookupService}

// SnowPlow ETL
import IPLocation._

/**
 * IpGeo is a wrapper around MaxMind's own LookupService.
 *
 * Two main differences:
 *
 * 1. getLocation(ip: String) now returns a stringly-typed
 *    IpLocation case class, not a raw MaxMind Location
 * 2. IpGeo introduces a 10k-element LRU cache to improve
 *    lookup performance
 *
 * Inspired by:
 * https://github.com/jt6211/hadoop-dns-mining/blob/master/src/main/java/io/covert/dns/geo/IpGeo.java
 */
class IpGeo(dbFile: String, fromDisk: Boolean = false) {

	// Initialise the cache
	private val lru = new LruMap[String, IpLocation](10000) // Of type mutable.Map[String, IpLocation]

	// Configure the lookup service
	private val options = if (fromDisk) LookupService.GEOIP_STANDARD else LookupService.GEOIP_MEMORY_CACHE
	private val maxmind = new LookupService(dbFile, options)

	/**
	 * Returns the MaxMind location for this IP address.
	 * If MaxMind can't find the IP address, then return
	 * an empty location.
	 */
	def getLocation(ip: String): IpLocation = {

		val cached = lru.get(ip)
		if (cached != null) {
			return cached
		}

		val location = maxmind.getLocation(ip)
		if (location == null) {
			lru.put(ip, NoLocation)
			return NoLocation
		}

		lru.put(ip, location)
		location
	}
}