/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
/*
package com.snowplowanalytics.snowplow.enrich.common
package enrichments

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object ConfigEnrichments {

	def getAnonIpOctets(anonIpJson: JsonNode): String = {
		if (anonIpJson.get("enabled")) anonIpJson.get("parameters").get("anon_octets")
		else "0" // Anonymize 0 quartets == anonymization disabled
	}

  /**
   * Convert a Stringly-typed integer
   * into the corresponding AnonOctets
   * Enum Value.
   *
   * Update the Validation Error if the
   * conversion isn't possible.
   *
   * @param anonOctets A String holding
   *        the number of IP address
   *        octets to anonymize
   * @return a Validation-boxed AnonOctets
   */
  private def getAnonOctets(anonOctets: String): Validation[String, AnonOctets] = {
    try {
      AnonOctets.withName(anonOctets).success
    } catch {
      case nse: NoSuchElementException => "IP address octets to anonymize must be 0, 1, 2, 3 or 4".fail
    }
  }


}*/