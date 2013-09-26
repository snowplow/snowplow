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

/**
 * Contains enrichments related to ensuring
 * user privacy.
 */
object PrivacyEnrichments {

  /**
   * How many quartets to anonymize?
   */
  object AnonQuartets extends Enumeration {
    type AnonQuartets = Value
    val None  = Value(0)
    val One   = Value(1)
    val Two   = Value(2)
    val Three = Value(3)
    val All   = Value(4)
  }

  /**
   * Anonymize the supplied IP address.
   *
   * quartets is the number of quartets
   * in the IP address to anonymize, starting
   * from the right. For example:
   *
   * anonymizeIp("94.15.223.151", One)
   * => "94.15.223.x"
   *
   * anonymizeIp("94.15.223.151", Three)
   * => "94.x.x.x"
   *
   * @param ip The IP address to anonymize
   * @param quartets The number of quartets
   *        to anonymize
   * @return the anonymized IP address
   */
  def anonymizeIp(ip: String, quartets: AnonQuartets): String = {
    ip // TODO: impl
  }
}