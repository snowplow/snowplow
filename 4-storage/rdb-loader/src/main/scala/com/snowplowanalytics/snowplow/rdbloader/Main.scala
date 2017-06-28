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

import cats.data.Validated._

import scala.util.control.NonFatal

// This project
import interpreters.Interpreter
import config.CliConfig
import loaders.Common.load

/**
 * Application entry point
 */
object Main {
  /**
   * If arguments or config is invalid exit with 1
   * and print errors to EMR stdout
   * If arguments and config are valid, but loading failed
   * print message to `track` bucket
   */
  def main(argv: Array[String]): Unit = {
    CliConfig.parse(argv) match {
      case Some(Valid(config)) =>
        run(config)
      case Some(Invalid(errors)) =>
        errors.toList.foreach(println)
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }

  def run(config: CliConfig) = {
    val interpreter = Interpreter.initialize(config)
    val actions = for {
      result     <- load(config).value.run(Nil)
      message     = utils.Common.interpretResult(result)
      _          <- LoaderA.track(message)
      dumpResult <- LoaderA.dump(message)
      _          <- LoaderA.exit(message, dumpResult) // exit(1) if dump wasn't successful
    } yield message

    try {
      actions.foldMap(interpreter.run)
    } catch {
      case NonFatal(e) =>
        println(e)
        sys.exit(1)
    }
  }
}
