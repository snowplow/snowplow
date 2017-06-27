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
        val status = run(config)
        sys.exit(status)
      case Some(Invalid(errors)) =>
        println("Configuration error")
        errors.toList.foreach(error => println(error.message))
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }

  /**
   * Initalize interpreter from parsed configuration and
   * run all IO actions through it. Should never throw exceptions
   *
   * @param config parsed configuration
   * @return exit code status. 0 for success, 1 if anything went wrong
   */
  def run(config: CliConfig): Int = {
    val interpreter = Interpreter.initialize(config)

    val actions = for {
      result     <- load(config).value.run(Nil)
      message     = utils.Common.interpret(result)
      _          <- LoaderA.track(message)
      dumpResult <- LoaderA.dump(message)
      status     <- LoaderA.exit(message, dumpResult) // exit(1) if dump wasn't successful
    } yield status

    actions.foldMap(interpreter.run)
  }
}
