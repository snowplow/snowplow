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

// MaxMind
import com.maxmind.geoip.Location

/**
 * A stringly-typed case class wrapper around the
 * MaxMind Location class.
 * Stringly-typed because these fields will be
 * written to flatfile by Scalding.
 */
case class IpLocation(
	countryCode: String,
	countryName: String,
	region: String,
	city: String,
	postalCode: String,
	latitude: String,
	longitude: String,
	dmaCode: String,
	areaCode: String,
	metroCode: String
	)

/**
 * Helpers for an IpLocation
 */
object IpLocation {
	
	// Contents of each missing field 
	private val Empty = ""

	// Helpers to convert float or string into empty field for IpLocation
	private val stringifyInt: Int => String = i => if (i == 0) Empty else i.toString()
	private val stringifyFloat: Float => String = fl => if (fl == 0.0f) Empty else fl.toString()

	// Represents an unidentified location
	val UnknownIpLocation = IpLocation(Empty, Empty, Empty, Empty, Empty, Empty, Empty, Empty, Empty, Empty)

	/**
	 * Converts MaxMind Location to a stringly-typed IpLocation
	 */
	implicit def location2IpLocation(loc: Location): IpLocation = IpLocation(
		countryCode = Option(loc.countryCode).getOrElse(Empty),
		countryName = loc.countryName,
		region = loc.region,
		city = loc.city,
		postalCode = loc.postalCode,
		latitude = stringifyFloat(loc.latitude),
		longitude = stringifyFloat(loc.longitude),
		dmaCode = stringifyInt(loc.dma_code),
		areaCode = stringifyInt(loc.area_code),
		metroCode = stringifyInt(loc.metro_code)
		)
}