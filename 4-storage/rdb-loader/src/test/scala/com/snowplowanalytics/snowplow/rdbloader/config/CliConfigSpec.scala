/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package config

import cats.data.Validated

// specs2
import org.specs2.Specification

import S3.Key.{coerce => s3}
import LoaderError.{ConfigError, DecodingError, ValidationError}

class CliConfigSpec extends Specification { def is = s2"""
  Parse minimal valid configuration $e1
  Collect custom steps $e2
  Aggregate errors $e3
  Return None on invalid CLI options $e4
  """

  import SpecHelpers._

  def e1 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolver,
      "--target", target,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] =
      Set(Step.Delete, Step.Load, Step.Analyze, Step.Shred, Step.Download, Step.Discover)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef"))

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e2 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolver,
      "--skip", "shred,delete",
      "--target", target,
      "-i", "vacuum",
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef")

    val expectedSteps: Set[Step] = Set(
      Step.Load,
      Step.Analyze,
      Step.Download,
      Step.Compupdate,
      Step.Vacuum,
      Step.Discover)

    val expected = CliConfig(validConfig, validTarget, expectedSteps, s3("s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef"))

    val result = CliConfig.parse(cli)

    result must beSome(Validated.Valid(expected))
  }

  def e3 = {
    val cli = Array(
      "--config", invalidConfigYml,
      "--resolver", resolver,
      "--logkey", "s3://log-bucket/run=2017-04-12-10-01-02/abcdef-1234-8912-abcdef",
      "--skip", "shred,delete",
      "--target", invalidTarget,
      "-i", "vacuum,compupdate")

    val result = CliConfig.parse(cli)

    result must beSome.like {
      case Validated.Invalid(nel) =>
        val validationError = nel.toList must contain((error: ConfigError) => error.isInstanceOf[ValidationError])
        val decodingError = nel.toList must contain((error: ConfigError) => error.isInstanceOf[DecodingError])
        validationError.and(decodingError)
    }
  }


  def e4 = {
    val cli = Array(
      "--config", configYml,
      "--resolver", resolver,
      "--skip", "shred,delete",
      "--target", target,
      "-i", "vacuum,nosuchstep")

    val result = CliConfig.parse(cli)

    result must beNone
  }
}
