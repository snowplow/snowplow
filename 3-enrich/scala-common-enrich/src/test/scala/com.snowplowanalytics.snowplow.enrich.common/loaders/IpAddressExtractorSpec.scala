/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class IpAddressExtractorSpec extends Specification with DataTables with ValidationMatchers {

  val Default = "255.255.255.255"

  "extractIpAddress" should {
    "correctly extract an X-FORWARDED-FOR header" in {

      "SPEC NAME"                            || "HEADERS"                                               | "EXP. RESULT" |
        "No headers"                         !! Nil                                                     ! Default |
        "No X-FORWARDED-FOR header"          !! List("Accept-Charset: utf-8", "Connection: keep-alive") ! Default |
        "Unparseable X-FORWARDED-FOR header" !! List("X-Forwarded-For: localhost")                      ! Default |
        "Good X-FORWARDED-FOR header" !! List("Accept-Charset: utf-8",
                                              "X-Forwarded-For: 129.78.138.66, 129.78.64.103",
                                              "Connection: keep-alive") ! "129.78.138.66" |
        "Good incorrectly capitalized X-FORWARDED-FOR header" !! List("Accept-Charset: utf-8",
                                                                      "x-FoRwaRdeD-FOr: 129.78.138.66, 129.78.64.103",
                                                                      "Connection: keep-alive") ! "129.78.138.66"                                                   |
        "IPv6 address in X-FORWARDED-FOR header"                                                !! List("X-Forwarded-For: 2001:0db8:85a3:0000:0000:8a2e:0370:7334") ! "2001:0db8:85a3:0000:0000:8a2e:0370:7334" |
        "IPv6 quoted address in X-FORWARDED-FOR header"                     !! List(
          "X-Forwarded-For: \"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]\"") ! "2001:0db8:85a3:0000:0000:8a2e:0370:7334" |
        "IPv4 address in Forwarded header"                                  !! List("Forwarded: for=129.78.138.66, 129.78.64.103") ! "129.78.138.66" |
        "IPv6 incorrectly quoted address in Forwarded header"                          !! List(
          "Forwarded: for=2001:0db8:85a3:0000:0000:8a2e:0370:7334, for=129.78.138.56") ! "2001:0db8:85a3:0000:0000:8a2e:0370:7334" |
        "IPv6 quoted correctly in Forwarded header"                                          !! List(
          "Forwarded: for=\"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]\", \"129.78.128.66\"") ! "2001:0db8:85a3:0000:0000:8a2e:0370:7334" |> {
        (_, headers, expected) =>
          {
            IpAddressExtractor.extractIpAddress(headers, Default) must_== expected
          }
      }
    }

    "prioritize X-Forwarded-For over Forwarded: for=" in {
      val ipXForwarded   = "1.1.1.1"
      val ipForwardedFor = "2.2.2.2"
      IpAddressExtractor.extractIpAddress(List(s"Forwarded: for=$ipForwardedFor", s"X-Forwarded-For: $ipXForwarded"),
                                          Default) must_== ipXForwarded
    }

    "remove port if any" in {
      val ipv4         = "1.1.1.1"
      val ipv4WithPort = s"$ipv4:8080"
      IpAddressExtractor.extractIpAddress(List(s"X-Forwarded-For: $ipv4WithPort"), Default) must_== ipv4

      val ipv6         = "1fff:0:a88:85a3::ac1f"
      val ipv6WithPort = s"[$ipv6]:8001"
      IpAddressExtractor.extractIpAddress(List(s"X-Forwarded-For: $ipv6WithPort"), Default) must_== ipv6
    }

    "correctly extract an X-FORWARDED-FOR Cloudfront field" in {

      "SPEC NAME"                          || "Field"                          | "EXP. RESULT" |
        "No X-FORWARDED-FOR field"         !! "-"                              ! Default |
        "Incorrect X-FORWARDED-FOR field"  !! "incorrect"                      ! Default |
        "One IP in X-FORWARDED-FOR field"  !! "129.78.138.66"                  ! "129.78.138.66" |
        "Two IPs in X-FORWARDED-FOR field" !! "129.78.138.66,%20129.78.64.103" ! "129.78.138.66" |> {

        (_, xForwardedFor, expected) =>
          {
            IpAddressExtractor.extractIpAddress(xForwardedFor, Default) must_== expected
          }
      }
    }
  }
}
