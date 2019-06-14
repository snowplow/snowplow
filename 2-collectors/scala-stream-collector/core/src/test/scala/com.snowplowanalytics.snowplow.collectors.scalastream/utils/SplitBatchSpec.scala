/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream
package utils

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.snowplow.badrows._
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import org.apache.thrift.TDeserializer
import org.specs2.mutable.Specification

import model.SplitBatchResult

class SplitBatchSpec extends Specification {
  "SplitBatch.split" should {
    "Batch a list of strings based on size" in {
      SplitBatch.split(List("a", "b", "c").map(Json.fromString), 9, 1) must_==
        SplitBatchResult(
          List(List("c"),List("b", "a")).map(_.map(Json.fromString)),
          Nil)
    }

    "Reject only those strings which are too big" in {
      SplitBatch.split(List("1234567", "1", "123").map(Json.fromString), 8, 0) must_==
        SplitBatchResult(
          List(List("123", "1").map(Json.fromString)),
          List("1234567").map(Json.fromString))
    }

    "Batch a long list of strings" in {
      SplitBatch.split(List("123456778901", "123456789", "12345678", "1234567", "123456", "12345", "1234", "123", "12", "1").map(Json.fromString), 13, 0) must_==
        SplitBatchResult(
          List(
            List("1", "12", "123"),
            List("1234", "12345"),
            List("123456"),
            List("1234567"),
            List("12345678"),
            List("123456789")).map(_.map(Json.fromString)),
          List("123456778901").map(Json.fromString))
    }
  }

  "SplitBatch.splitAndSerializePayload" should {
    "Serialize an empty CollectorPayload" in {
      val actual = SplitBatch.splitAndSerializePayload(new CollectorPayload(), 100)
      val target = new CollectorPayload()
      new TDeserializer().deserialize(target, actual.good.head)
      target must_== new CollectorPayload()
    }

    "Reject an oversized GET CollectorPayload" in {
      val payload = new CollectorPayload()
      payload.setQuerystring("x" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 100)
      val res = parse(new String(actual.bad.head)).toOption.get
      val badRow = res.as[BadRow].toOption.get
      badRow must beAnInstanceOf[BadRow.SizeViolation]
      val sizeViolation = badRow.asInstanceOf[BadRow.SizeViolation]
      sizeViolation.failure.maximumAllowedSizeBytes must_== 100
      sizeViolation.failure.actualSizeBytes must_== 1019
      sizeViolation.failure.expectation must_== "oversized collector payload: GET requests cannot be split"
      sizeViolation.payload.line must_== "CollectorP"
      sizeViolation.processor shouldEqual Processor(generated.BuildInfo.name, generated.BuildInfo.version)
      actual.good must_== Nil
    }

    "Reject an oversized POST CollectorPayload with an unparseable body" in {
      val payload = new CollectorPayload()
      payload.setBody("s" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 100)
      val res = parse(new String(actual.bad.head)).toOption.get
      val badRow = res.as[BadRow].toOption.get
      badRow must beAnInstanceOf[BadRow.SizeViolation]
      val sizeViolation = badRow.asInstanceOf[BadRow.SizeViolation]
      sizeViolation.failure.maximumAllowedSizeBytes must_== 100
      sizeViolation.failure.actualSizeBytes must_== 1019
      sizeViolation.failure.expectation must_== "oversized collector payload: cannot split POST requests which are not json expected json value got 'ssssss...' (line 1, column 1)"
      sizeViolation.payload.line must_== "CollectorP"
      sizeViolation.processor shouldEqual Processor(generated.BuildInfo.name, generated.BuildInfo.version)
    }

    "Reject an oversized POST CollectorPayload which would be oversized even without its body" in {
      val payload = new CollectorPayload()
      val data = Json.obj(
        "schema" := Json.fromString("s"),
        "data" := Json.arr(
          Json.obj("e" := "se", "tv" := "js"),
          Json.obj("e" := "se", "tv" := "js")
        )
      )
      payload.setBody(data.noSpaces)
      payload.setPath("p" * 1000)
      val actual = SplitBatch.splitAndSerializePayload(payload, 1000)
      actual.bad.size must_== 1
      val res = parse(new String(actual.bad.head)).toOption.get
      val badRow = res.as[BadRow].toOption.get
      badRow must beAnInstanceOf[BadRow.SizeViolation]
      val sizeViolation = badRow.asInstanceOf[BadRow.SizeViolation]
      sizeViolation.failure.maximumAllowedSizeBytes must_== 1000
      sizeViolation.failure.actualSizeBytes must_== 1091
      sizeViolation.failure.expectation must_== "oversized collector payload: cannot split POST requests which are not self-describing INVALID_IGLUURI"
      sizeViolation.payload.line must_== "CollectorPayload(schema:null, ipAddress:null, timestamp:0, encoding:null, collector:null, path:ppppp"
      sizeViolation.processor shouldEqual Processor(generated.BuildInfo.name, generated.BuildInfo.version)
    }

    "Split a CollectorPayload with three large events and four very large events" in {
      val payload = new CollectorPayload()
      val data = Json.obj(
        "schema" := Schemas.SizeViolation.toSchemaUri,
        "data" := Json.arr(
          Json.obj("e" := "se", "tv" := "x" * 600),
          Json.obj("e" := "se", "tv" := "x" * 5),
          Json.obj("e" := "se", "tv" := "x" * 600),
          Json.obj("e" := "se", "tv" := "y" * 1000),
          Json.obj("e" := "se", "tv" := "y" * 1000),
          Json.obj("e" := "se", "tv" := "y" * 1000),
          Json.obj("e" := "se", "tv" := "y" * 1000)
        )
      )
      payload.setBody(data.noSpaces)
      val actual = SplitBatch.splitAndSerializePayload(payload, 1000)
      actual.bad.size must_== 4
      actual.good.size must_== 2
    }
  }
}
