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

import cats.data._
import cats.implicits._

import java.util.Base64
import java.nio.charset.StandardCharsets

// This project
import LoaderError._
import generated.ProjectMetadata
import utils.{ Common, Compat }

/**
 * Validated and parsed result application config
 *
 * @param configYaml decoded Snowplow config.yml
 * @param target decoded target to load
 * @param steps collected steps
 */
case class CliConfig(
  configYaml: SnowplowConfig,
  target: StorageTarget,
  steps: Set[Step],
  logKey: S3.Key)

object CliConfig {

  val parser = new scopt.OptionParser[RawConfig]("rdb-loader") {
    head("Relational Database Loader", ProjectMetadata.version)

    opt[String]('c', "config").required().valueName("<config.yml>").
      action((x, c) ⇒ c.copy(config = x)).
      text("base64-encoded string with config.yml content")

    opt[String]('t', "target").required().valueName("<target.json>").
      action((x, c) => c.copy(target = x)).
      text("base64-encoded string with single storage target configuration JSON")

    opt[String]('r', "resolver").required().valueName("<resolver.json>").
      action((x, c) => c.copy(resolver = x)).
      text("base64-encoded string with Iglu resolver configuration JSON")

    opt[String]('l', "logkey").required().valueName("<name>").
      action((x, c) => c.copy(logkey = x)).
      text("base64-encoded string with Iglu resolver configuration JSON")

    opt[Seq[Step.IncludeStep]]('i', "include").action((x, c) ⇒
      c.copy(include = x)).text("include optional work steps")

    opt[Seq[Step.SkipStep]]('s', "skip").action((x, c) =>
      c.copy(skip = x)).text("skip default steps")

    help("help").text("prints this usage text")

  }

  /**
   * Parse raw CLI arguments into validated and transformed application config
   * This is side-effecting function, it'll print to stdout all errors
   *
   * @param argv list of command-line arguments
   * @return none if not all required arguments were passed
   *         or unknown arguments provided,
   *         some config error if arguments could not be transformed
   *         into application config
   *         some application config if everything was validated
   *         correctly
   */
  def parse(argv: Seq[String]): Option[ValidatedNel[ConfigError, CliConfig]] =
    parser.parse(argv, rawCliConfig).map(transform)


  /**
   * Initial raw configuration parsed from CLI arguments
   * Could be invalid, supposed to be validated and transformed
   * into `CliConfig`
   *
   * @param config base64-encoded Snowplow config.yml
   * @param target base64-encoded storage target JSON
   * @param resolver base64-encoded Iglu Resolver JSON
   * @param include sequence of of decoded steps to include
   * @param skip sequence of of decoded steps to skip
   * @param logkey filename, where RDB log dump will be saved
   */
  private[config] case class RawConfig(
    config: String,
    target: String,
    resolver: String,
    include: Seq[Step.IncludeStep],
    skip: Seq[Step.SkipStep],
    logkey: String)

  // Always invalid initial parsing configuration
  private[this] val rawCliConfig = RawConfig("", "", "", Nil, Nil, "")

  /**
   * Validated and transform initial raw cli arguments into
   * ready-to-use `CliConfig`, aggregating errors if any
   *
   * @param cliConfig initial raw arguments
   * @return application config in case of success or
   *         non empty list of config errors in case of failure
   */
  private[config] def transform(cliConfig: RawConfig): ValidatedNel[ConfigError, CliConfig] = {
    val config = base64decode(cliConfig.config).flatMap(SnowplowConfig.parse).toValidatedNel
    val logkey = S3.Key.parse(cliConfig.logkey).leftMap(DecodingError).toValidatedNel
    val target = loadTarget(cliConfig.resolver, cliConfig.target)
    val steps = Step.constructSteps(cliConfig.skip.toSet, cliConfig.include.toSet)

    (target |@| config |@| logkey).map { case (t, c, l) => CliConfig(c, t, steps, l) }
  }

  /**
   * Safely decode base64 string into plain-text string
   *
   * @param string string, supposed to be base64-encoded
   * @return either error with full desciption or
   *         plain string in case of success
   */
  private def base64decode(string: String): Either[ConfigError, String] = {
    try {
      Right(new String(Base64.getDecoder.decode(string), StandardCharsets.UTF_8))
    } catch {
      case e: IllegalArgumentException =>
        Left(ParseError(e.getMessage))
    }
  }

  /**
   * Decode and validate base64-encoded storage target config JSON
   *
   * @param resolverConfigB64 base64-encoded Iglu resolver config
   * @param targetConfigB64 base64-encoded storage target JSON
   * @return either aggregated list of errors (from both resolver and target)
   *         or successfuly decoded storage target
   */
  private def loadTarget(resolverConfigB64: String, targetConfigB64: String) = {
    val json = base64decode(resolverConfigB64).flatMap(Common.safeParse).toValidatedNel
    val resolver = json.andThen(Compat.convertIgluResolver)
    val decodedTarget = base64decode(targetConfigB64).toValidatedNel
    (resolver |@| decodedTarget).tupled.andThen {
      case (r, t) => StorageTarget.parseTarget(r, t)
    }
  }
}
