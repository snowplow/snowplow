/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.utils

import com.snowplowanalytics.iglu.client.ClientError.{ResolutionError, ValidationError}
import com.snowplowanalytics.iglu.client.validator.ValidatorError.InvalidData
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.badrows.FailureDetails.SchemaViolation.{
  CriterionMismatch,
  IgluError
}

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.utils.Clock._

class ShredderSpec extends Specification {
  "extractAndValidateUnstructEvent" should {
    "return successful none for empty event" >> {
      Shredder
        .extractAndValidateUnstructEvent(new EnrichedEvent, SpecHelpers.client)
        .value must beRight(None)
    }

    "return a failure for wrong schema" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1", "data": []}"""
      )
      val expected = CriterionMismatch(
        SchemaKey(
          "com.snowplowanalytics.snowplow",
          "contexts",
          "jsonschema",
          SchemaVer.Full(1, 0, 1)
        ),
        SchemaCriterion(
          "com.snowplowanalytics.snowplow",
          "unstruct_event",
          "jsonschema",
          Some(1),
          Some(0),
          None
        )
      )
      Shredder.extractAndValidateUnstructEvent(input, SpecHelpers.client).value must beLeft(
        expected
      )
    }

    "return a failure for invalid unstruect_event payload" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0", "data": []}"""
      )
      val ExpectedSchema = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "unstruct_event",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      Shredder.extractAndValidateUnstructEvent(input, SpecHelpers.client).value must beLeft.like {
        case IgluError(ExpectedSchema, ValidationError(InvalidData(_))) => ok
        case _ => ko(s"Unexpected IgluError")
      }
    }

    "return a failure for unresolved inner schema" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0", "data": {"schema": "iglu:com.acme/foo/jsonschema/1-0-0", "data": {}}}"""
      )
      val ExpectedSchema = SchemaKey(
        "com.acme",
        "foo",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      Shredder.extractAndValidateUnstructEvent(input, SpecHelpers.client).value must beLeft.like {
        case IgluError(ExpectedSchema, ResolutionError(_)) => ok
        case _ => ko(s"Unexpected IgluError")
      }
    }
  }

  "extractAndValidateCustomContexts" should {
    "return successful Nil for empty event" >> {
      Shredder
        .extractAndValidateCustomContexts(new EnrichedEvent, SpecHelpers.client)
        .value must beRight(Nil)
    }

    "return a failure for empty com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0" >> {
      val input = new EnrichedEvent
      input.setContexts(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0", "data": []}"""
      )
      val ExpectedSchema = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "contexts",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      Shredder.extractAndValidateCustomContexts(input, SpecHelpers.client).value must beLeft.like {
        case IgluError(ExpectedSchema, ValidationError(InvalidData(_))) => ok
        case _ => ko("Unexpected IgluError")
      }
    }

    "attempt to validate inner context" >> {
      val input = new EnrichedEvent
      input.setContexts(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1", "data": [{"schema": "iglu:com.acme/foo/jsonschema/1-0-0", "data": {}}]}"""
      )
      val ExpectedSchema = SchemaKey(
        "com.acme",
        "foo",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      Shredder.extractAndValidateCustomContexts(input, SpecHelpers.client).value must beLeft.like {
        case IgluError(ExpectedSchema, ResolutionError(_)) => ok
        case _ => ko("Unexpected IgluError")
      }
    }
  }
}
