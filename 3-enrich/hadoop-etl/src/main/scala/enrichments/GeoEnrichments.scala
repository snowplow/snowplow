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
package com.snowplowanalytics.snowplow.enrich.hadoop
package enrichments

// Scalaz
import scalaz._
import Scalaz._

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.{IpGeo, IpLocation}

/**
 * Contains enrichments related to geo-location.
 */
object GeoEnrichments {

  /**
   * Extract the geo-location using the
   * client IP address.
   *
   * Note we wrap the getLocation call in a try
   * catch block. At the time of writing, no
   * valid or invalid IP address can make
   * getLocation throw an Exception, but we keep
   * this protection in case this situation
   * changes in the future (as we don't control
   * the functionality of the underlying MaxMind
   * Java API).
   *
   * @param geo The IpGeo lookup engine we will
   *        use to lookup the client's IP address
   * @param ip The client's IP address to use to
   *        lookup the client's geo-location
   * @return a MaybeIpLocation (Option-boxed
   *         IpLocation), or an error message,
   *         boxed in a Scalaz Validation
   */
  // TODO: can we move the IpGeo to an implicit?
  def extractGeoLocation(geo: IpGeo, ip: String): Validation[String, MaybeIpLocation] = {

    try {
      geo.getLocation(ip).success
    } catch {
      case _ => return "Could not extract geo-location from IP address [%s]".format(ip).fail
    }
  }
}