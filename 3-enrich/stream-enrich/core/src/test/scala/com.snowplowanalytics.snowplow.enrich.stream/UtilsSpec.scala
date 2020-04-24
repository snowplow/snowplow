/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.stream
package sources

import com.snowplowanalytics.snowplow.enrich.stream.model.{
  AWSCredentials,
  DualCloudCredentialsPair,
  GCPCredentials,
  NoCredentials
}
import org.specs2.mutable.Specification

class UtilsSpec extends Specification {
  "validatePii" should {
    "return left if the enrichment is on and there is no stream name" in {
      utils.validatePii(true, None) must beLeft(
        "PII was configured to emit, but no PII stream name was given"
      )
    }

    "return right otherwise" in {
      utils.validatePii(true, Some("s")) must beRight(())
      utils.validatePii(false, Some("s")) must beRight(())
      utils.validatePii(false, None) must beRight(())
    }
  }

  "emitPii" should {
    "return true if the emit event enrichment setting is true" in {
      utils.emitPii(SpecHelpers.enrichmentRegistry) must_== true
    }
  }

  "extractCredentials" should {
    "extract optional AWS and GCP credential from cloud agnostic configuration" in {
      utils.extractCredentials(SpecHelpers.kafkaConfig) mustEqual DualCloudCredentialsPair(
        AWSCredentials("access1", "secret1"),
        NoCredentials
      )
      utils.extractCredentials(SpecHelpers.nsqConfigWithoutCreds) mustEqual DualCloudCredentialsPair(
        NoCredentials,
        NoCredentials
      )
      utils.extractCredentials(SpecHelpers.nsqConfigWithCreds) mustEqual DualCloudCredentialsPair(
        AWSCredentials("access2", "secret2"),
        GCPCredentials("credsPath1")
      )
      utils.extractCredentials(SpecHelpers.stdinConfig) mustEqual DualCloudCredentialsPair(
        NoCredentials,
        GCPCredentials("credsPath2")
      )
    }
  }
}
