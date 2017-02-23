/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.rdbloader

import java.io.File

object Main extends App {

  sealed trait WorkStep extends Product with Serializable
  case object Compupdate extends WorkStep
  case object Vacuum extends WorkStep

  sealed trait Step extends Product with Serializable // Other kind of step
  case object ArchiveEnriched extends Step
  case object Download extends Step
  case object Analyze extends Step
  case object Delete extends Step
  case object Shred extends Step
  case object Load extends Step

  case class CliConfig(config: File, b64config: Boolean, include: List[String], skip: List[String])

  case class Config() // Contains parsed configs

  val parser = new scopt.OptionParser[CliConfig]("scopt") {
    head("Relational Database Loader", "0.1.0") // TODO: from generated

    opt[File]('c', "config").required().valueName("<file>").
      action((x, c) ⇒ c.copy(config = x)).
      text("configuration file")

    opt[Unit]('b', "base64-config-string").action((_, c) ⇒
      c.copy(b64config = true)).text("base64-encoded configuration string")

    opt[Seq[String]]('i', "include").action((x, c) ⇒
      c.copy(include = x.toList)).text("include optional work steps")

    help("help").text("prints this usage text")

  }

  println("Hello Relational Database Loader!")
}
