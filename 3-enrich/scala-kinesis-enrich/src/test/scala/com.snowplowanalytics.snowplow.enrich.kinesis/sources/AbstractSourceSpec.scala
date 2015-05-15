/**
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this file via any medium is strictly prohibited.
 *
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics
package snowplow.enrich
package kinesis
package sources

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Specs2
import org.specs2.mutable.Specification

class AbstractSourceSpec extends Specification {

  "getSize" should {

    "get the size of a string of ASCII characters" in {

      AbstractSource.getSize("abcdefg") must_== 7
    }

    "get the size of a string containing non-ASCII characters" in {

      AbstractSource.getSize("™®字") must_== 8
    }
  }

  "adjustOversizedFailureJson" should {

    "remove the \"line\" field from a large bad JSON" in {

      val badJson = """{"line":"huge", "errors":["some error"], "other":"more information"}"""

      val parsed = parse(AbstractSource.adjustOversizedFailureJson(badJson))

      parsed \ "line" must_== JNothing
      parsed \ "other" must_== JString("more information")
      parsed \ "size" must_== JInt(AbstractSource.getSize(badJson))
    }

    "remove create a new bad row if the bad row JSON is unparseable" in {
      val badJson = "{"

      val parsed = parse(AbstractSource.adjustOversizedFailureJson(badJson))

      parsed \ "size" must_== JInt(1)
    }
  }

  "oversizedSuccessToFailure" should {

    "create a bad row JSON from an oversized success" in {

      AbstractSource.oversizedSuccessToFailure("abc", 100) must_==
         """{"size":3,"errors":["Enriched event size of 3 bytes is greater than allowed maximum of 100"]}"""
    }
  }
}
