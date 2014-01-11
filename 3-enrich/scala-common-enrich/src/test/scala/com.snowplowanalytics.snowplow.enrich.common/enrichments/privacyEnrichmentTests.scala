/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables

import PrivacyEnrichments._

/**
 * Tests the anonymzeIp function
 */
class AnonymizeIpTest extends Specification with DataTables {

  def is =
    "Anonymizing 0-4 octets across a variety of IP addresses should work" ! e1

  def e1 =
    "SPEC NAME"              || "IP ADDRESS"      | "ANONYMIZE QUARTETS"   | "EXPECTED OUTPUT"   |
    "valid, anonymize 0"     !! "168.23.101.20"   ! AnonOctets(0)        ! "168.23.101.20"     |
    "valid, anonymize 1"     !! "0.23.0.20"       ! AnonOctets(1)        ! "0.23.0.x"          |
    "valid, anonymize 2"     !! "168.192.102.4"   ! AnonOctets(2)        ! "168.192.x.x"       |
    "valid, anonymize 3"     !! "54.242.102.43"   ! AnonOctets(3)        ! "54.x.x.x"          |
    "valid, anonymize 4"     !! "94.15.213.171"   ! AnonOctets(4)        ! "x.x.x.x"           |
    "invalid, anonymize 1"   !! "777.2"           ! AnonOctets(1)        ! "777.2"             |
    "invalid, anonymize 2"   !! "777.2.23"        ! AnonOctets(4)        ! "x.x.x"             |
    "invalid, anonymize 3"   !! "999.123.777.2"   ! AnonOctets(3)        ! "999.x.x.x"         |
    "invalid, anonymize 4"   !! "hello;goodbye"   ! AnonOctets(3)        ! "hello;goodbye"     |
    "empty, anonymize 2"     !! null              ! AnonOctets(2)        ! null                |
    "empty, anonymize 4"     !! ""                ! AnonOctets(4)        ! "x"                 |> {
      (_, ip, quartets, expected) => PrivacyEnrichments.anonymizeIp(ip, quartets) must_== expected
    }
}
