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

// Java
import java.io.File

// Specs2
import org.specs2.mutable.Specification

class IpGeoTest extends Specification {

	val ip = "213.52.50.8"
	val expected = IpLocation(
		countryCode = "NO",
		countryName = "Norway",
		region = "01",
		city = "Ås",
		postalCode = "",
		latitude = "59.666702",
		longitude = "10.800003",
		dmaCode = "",
		areaCode = "",
		metroCode = ""
		)

	"The IP address %s".format(ip) should {

		val actual = IpGeoTest.createIpGeo.getLocation(ip)

		"have countryCode = %s".format(expected.countryCode) in {
			actual.countryCode must_== expected.countryCode
		}
		"have countryName = %s".format(expected.countryName) in {
			actual.countryName must_== expected.countryName
		}
		"have region = %s".format(expected.region) in {
			actual.region must_== expected.region
		}
		"have city = %s".format(expected.city) in {
			actual.city must_== expected.city
		}
		"have postalCode = %s".format(expected.postalCode) in {
			actual.postalCode must_== expected.postalCode
		}
		"have latitude = %s".format(expected.latitude) in {
			actual.latitude must_== expected.latitude
		}
		"have longitude = %s".format(expected.longitude) in {
			actual.longitude must_== expected.longitude
		}
		"have dmaCode = %s".format(expected.dmaCode) in {
			actual.dmaCode must_== expected.dmaCode
		}
		"have areaCode = %s".format(expected.areaCode) in {
			actual.areaCode must_== expected.areaCode
		}
		"have metroCode = %s".format(expected.metroCode) in {
			actual.metroCode must_== expected.metroCode
		}
	}
}

object IpGeoTest {

	def createIpGeo: IpGeo = {
		val dbFilepath = getClass.getResource("/maxmind/GeoLiteCity.dat").toURI()
		new IpGeo(dbFile = new File(dbFilepath), fromDisk = false)
	}
}