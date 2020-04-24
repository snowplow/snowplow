/**
 * Copyright (c) 2019-2020 Snowplow Analytics Ltd. All rights reserved.
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
package registry

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import io.circe.parser._
import io.circe.literal._

import nl.basjes.parse.useragent.UserAgent

import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

class YauaaEnrichmentSpec extends Specification with ValidatedMatchers {

  import YauaaEnrichment.decapitalize

  val yauaaEnrichment = YauaaEnrichment(None)

  // Devices
  val uaIpad =
    "Mozilla/5.0 (iPad; CPU OS 6_1_3 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10B329 Safari/8536.25"
  val uaIphoneX =
    "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
  val uaIphone7 =
    "Mozilla/5.0 (iPhone9,3; U; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1"
  val uaGalaxyTab =
    "Mozilla/5.0 (Linux; U; Android 2.2; fr-fr; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
  val uaGalaxyS9 =
    "Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36"
  val uaGalaxyS8 =
    "Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/60.0.3112.107 Mobile Safari/537.36"
  val uaXperiaXZ =
    "Mozilla/5.0 (Linux; Android 7.1.1; G8231 Build/41.2.A.0.219; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36"
  val uaNexusOne =
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
  val uaNexusS =
    "Mozilla/5.0 (Linux; Android 4.1.2; Nexus S Build/JZO54K) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19"
  val uaPlaystation4 = "Mozilla/5.0 (PlayStation 4 1.52) AppleWebKit/536.26 (KHTML, like Gecko)"

  // Browsers
  val uaChrome =
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1"
  val uaChromium =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Ubuntu Chromium/25.0.1364.160 Chrome/25.0.1364.160 Safari/537.22"
  val uaFirefox = "Mozilla/5.0 (Windows NT 6.1; rv:2.0b7pre) Gecko/20100921 Firefox/4.0b7pre"
  val uaIE = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)"
  val uaOpera =
    "Mozilla/4.0 (compatible; MSIE 6.0; MSIE 5.5; Windows NT 5.0) Opera 7.02 Bork-edition [en]"
  val uaSafari =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/536.30.1 (KHTML, like Gecko) Version/6.0.5 Safari/536.30.1"

  // Google bot
  val uaGoogleBot = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

  "YAUAA enrichment should" >> {
    "return default value for null" >> {
      yauaaEnrichment.parseUserAgent(null) shouldEqual yauaaEnrichment.defaultResult
    }

    "return default value for empty user agent" >> {
      yauaaEnrichment.parseUserAgent("") shouldEqual yauaaEnrichment.defaultResult
    }

    "detect correctly DeviceClass" >> {
      checkYauaaParsingForField(
        Map(
          uaChromium -> "Desktop",
          uaGalaxyTab -> "Tablet",
          uaIpad -> "Tablet",
          uaNexusS -> "Phone",
          uaPlaystation4 -> "Game Console",
          uaGoogleBot -> "Robot"
        ),
        UserAgent.DEVICE_CLASS
      )
    }

    "detect correctly DeviceName" >> {
      checkYauaaParsingForField(
        Map(
          uaGalaxyS9 -> "Samsung SM-G960F",
          uaGalaxyS8 -> "Samsung SM-G892A",
          uaXperiaXZ -> "Sony G8231",
          uaIphone7 -> "Apple iPhone",
          uaIphoneX -> "Apple iPhone",
          uaNexusS -> "Google Nexus S",
          uaNexusOne -> "Google Nexus ONE",
          uaPlaystation4 -> "Sony PlayStation 4",
          uaGoogleBot -> "Google"
        ),
        UserAgent.DEVICE_NAME
      )
    }

    "detect correctly OperatingSystemClass" >> {
      checkYauaaParsingForField(
        Map(
          uaChromium -> "Desktop",
          uaIE -> "Desktop",
          uaIpad -> "Mobile",
          uaNexusS -> "Mobile",
          uaPlaystation4 -> "Game Console",
          uaGoogleBot -> "Cloud"
        ),
        UserAgent.OPERATING_SYSTEM_CLASS
      )
    }

    "detect correctly OperatingSystemName" >> {
      checkYauaaParsingForField(
        Map(
          uaChromium -> "Ubuntu",
          uaIE -> "Windows NT",
          uaNexusOne -> "Android",
          uaIphoneX -> "iOS",
          uaIpad -> "iOS",
          uaSafari -> "Mac OS X",
          uaGoogleBot -> "Google"
        ),
        UserAgent.OPERATING_SYSTEM_NAME
      )
    }

    "detect correctly LayoutEngineClass" >> {
      checkYauaaParsingForField(
        Map(
          uaChrome -> "Browser",
          uaIE -> "Browser",
          uaNexusOne -> "Browser",
          uaIphoneX -> "Browser",
          uaIpad -> "Browser",
          uaSafari -> "Browser",
          uaPlaystation4 -> "Browser",
          uaGoogleBot -> "Robot"
        ),
        UserAgent.LAYOUT_ENGINE_CLASS
      )
    }

    "detect correctly AgentClass" >> {
      checkYauaaParsingForField(
        Map(
          uaChromium -> "Browser",
          uaIE -> "Browser",
          uaNexusOne -> "Browser",
          uaIphoneX -> "Browser",
          uaIpad -> "Browser",
          uaSafari -> "Browser",
          uaPlaystation4 -> "Browser",
          uaGoogleBot -> "Robot"
        ),
        UserAgent.AGENT_CLASS
      )
    }

    "detect correctly AgentName" >> {
      checkYauaaParsingForField(
        Map(
          uaChrome -> "Chrome",
          uaChromium -> "Chromium",
          uaFirefox -> "Firefox",
          uaIE -> "Internet Explorer",
          uaNexusOne -> "Stock Android Browser",
          uaIphoneX -> "Safari",
          uaIpad -> "Safari",
          uaOpera -> "Opera",
          uaSafari -> "Safari",
          uaGoogleBot -> "Googlebot"
        ),
        UserAgent.AGENT_NAME
      )
    }

    "create a JSON with the schema and the data" >> {
      val expected =
        SelfDescribingData(
          yauaaEnrichment.outputSchema,
          json"""{"deviceBrand":"Samsung","deviceName":"Samsung SM-G960F","layoutEngineNameVersion":"Blink 62.0","operatingSystemNameVersion":"Android 8.0.0","operatingSystemVersionBuild":"R16NW","layoutEngineNameVersionMajor":"Blink 62","operatingSystemName":"Android","agentVersionMajor":"62","layoutEngineVersionMajor":"62","deviceClass":"Phone","agentNameVersionMajor":"Chrome 62","operatingSystemClass":"Mobile","layoutEngineName":"Blink","agentName":"Chrome","agentVersion":"62.0.3202.84","layoutEngineClass":"Browser","agentNameVersion":"Chrome 62.0.3202.84","operatingSystemVersion":"8.0.0","agentClass":"Browser","layoutEngineVersion":"62.0"}"""
        )
      val actual = yauaaEnrichment.getYauaaContext(uaGalaxyS9)
      actual shouldEqual expected

      val defaultJson =
        SelfDescribingData(
          yauaaEnrichment.outputSchema,
          json"""{"deviceClass":"Unknown"}"""
        )
      yauaaEnrichment.getYauaaContext("") shouldEqual defaultJson
    }
  }

  /** Helper to check that a certain field of a parsed user agent has the expected value. */
  def checkYauaaParsingForField(expectedResults: Map[String, String], fieldName: String) =
    expectedResults.map {
      case (userAgent, expectedField) =>
        yauaaEnrichment.parseUserAgent(userAgent)(decapitalize(fieldName)) shouldEqual expectedField
    }.toList

  "decapitalize should" >> {
    "let an empty string as is" >> {
      decapitalize("") shouldEqual ""
    }

    "lower the unique character of a string" >> {
      decapitalize("A") shouldEqual "a"
      decapitalize("b") shouldEqual "b"
    }

    "lower only the first letter of a string" >> {
      decapitalize("FooBar") shouldEqual "fooBar"
      decapitalize("Foo Bar") shouldEqual "foo Bar"
      decapitalize("fooBar") shouldEqual "fooBar"
    }
  }

  "Parsing the config JSON for YAUAA enrichment" should {

    val schemaKey = SchemaKey(
      YauaaEnrichment.supportedSchema.vendor,
      YauaaEnrichment.supportedSchema.name,
      YauaaEnrichment.supportedSchema.format,
      SchemaVer.Full(1, 0, 0)
    )

    "successfully construct a YauaaEnrichment case class with the right cache size if specified" in {
      val cacheSize = 42

      val yauaaConfigJson = parse(s"""{
        "enabled": true,
        "parameters": {
          "cacheSize": $cacheSize
        }
      }""").toOption.get

      val expected = YauaaConf(Some(cacheSize))
      val actual = YauaaEnrichment.parse(yauaaConfigJson, schemaKey)
      actual must beValid(expected)
    }

    "successfully construct a YauaaEnrichment case class with a default cache size if none specified" in {
      val yauaaConfigJson = parse(s"""{
        "enabled": true
      }""").toOption.get

      val expected = YauaaConf(None)
      val actual = YauaaEnrichment.parse(yauaaConfigJson, schemaKey)
      actual must beValid(expected)
    }
  }
}
