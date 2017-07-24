/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package utils

import CollectorPayload.thrift.model1.CollectorPayload

// Thrift
import org.apache.thrift.TDeserializer

// json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s._

// Specs2
import org.specs2.mutable.Specification

import model.SplitBatchResult

class SplitBatchSpec extends Specification {
  args(skipAll = true)
  val splitBatch = SplitBatch

  "SplitBatch.split" should {
    "Batch a list of strings based on size" in {
      splitBatch.split(List("a", "b", "c"), 5, 1) must_==
        SplitBatchResult(
          List(List("c"),List("b", "a")),
          Nil)
    }

    "Reject only those strings which are too big" in {
      splitBatch.split(List("123456", "1", "123"), 5, 0) must_==
        SplitBatchResult(
          List(List("123", "1")),
          List("123456"))
    }

    "Batch a long list of strings" in {
      splitBatch.split(List("12345677890", "123456789", "12345678", "1234567", "123456", "12345", "1234", "123", "12", "1"), 9, 0) must_==
        SplitBatchResult(
          List(
            List("1", "12", "123"),
            List("1234", "12345"),
            List("123456"),
            List("1234567"),
            List("12345678"),
            List("123456789")),
          List("12345677890"))
    }
  }

  "SplitBatch.splitAndSerializePayload" should {
    "Serialize an empty CollectorPayload" in {
      val actual = SplitBatch.splitAndSerializePayload(new CollectorPayload(), 100)
      val target = new CollectorPayload
      new TDeserializer().deserialize(target, actual.good.head)
      target must_== new CollectorPayload
    }

    "Reject an oversized GET CollectorPayload" in {
      val payload = new CollectorPayload()
      payload.setQuerystring("x" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 100)
      val errorJson = parse(new String(actual.bad.head))
      errorJson \ "size" must_== JInt(1019)
      errorJson \ "errors" must_==
        JArray(List(JObject(List(("level", JString("error")), ("message", JString("Cannot split record with null body"))))))
      actual.good must_== Nil
    }

    "Reject an oversized POST CollectorPayload with an unparseable body" in {
      val payload = new CollectorPayload()
      payload.setBody("s" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 100)
      val errorJson = parse(new String(actual.bad.head))
      errorJson \ "size" must_== JInt(1019)
    }

    "Reject an oversized POST CollectorPayload which would be oversized even without its body" in {
      val payload = new CollectorPayload()
      val data = compact(render(
        ("schema" -> "s") ~
        ("data" -> List(
          ("e" -> "se") ~ ("tv" -> "js-2.4.3"),
          ("e" -> "se") ~ ("tv" -> "js-2.4.3")
        ))))
      payload.setBody(data)
      payload.setPath("p" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 1000)
      actual.bad.size must_== 1
      parse(new String(actual.bad.head)) \ "errors" must_==
        JArray(List(JObject(List(("level" ,JString("error")), ("message", JString("Even without the body, the serialized event is too large"))))))
    }   

    "Split a CollectorPayload with three large events and four very large events" in {
      val payload = new CollectorPayload()
      val data = compact(render(
        ("schema" -> "s") ~
        ("data" -> List(
          ("e" -> "se") ~ ("tv" -> "x" * 600),
          ("e" -> "se") ~ ("tv" -> "x" * 5),
          ("e" -> "se") ~ ("tv" -> "x" * 600),
          ("e" -> "se") ~ ("tv" -> "y" * 1000),
          ("e" -> "se") ~ ("tv" -> "y" * 1000),
          ("e" -> "se") ~ ("tv" -> "y" * 1000),
          ("e" -> "se") ~ ("tv" -> "y" * 1000)
          ))
        ))
      payload.setBody(data)
      val actual = SplitBatch.splitAndSerializePayload(payload, 1000)
      actual.bad.size must_== 4
      actual.good.size must_== 2
    }    
  }
}
