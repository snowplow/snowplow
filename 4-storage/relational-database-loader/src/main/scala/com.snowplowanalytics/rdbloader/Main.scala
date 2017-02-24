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



import Utils.StringEnum
import generated.ProjectMetadata

object Main extends App {

  import scopt.Read

  implicit val p = Read.reads { (Utils.fromString[OptionalWorkStep](_)).andThen(_.right.get) }

  sealed trait OptionalWorkStep extends StringEnum
  case object Compupdate extends OptionalWorkStep { def asString = "compupdate" }
  case object Vacuum extends OptionalWorkStep { def asString = "vacuum" }

  sealed trait SkippableStep extends Product with Serializable // Other kind of step
  case object ArchiveEnriched extends SkippableStep
  case object Download extends SkippableStep
  case object Analyze extends SkippableStep
  case object Delete extends SkippableStep
  case object Shred extends SkippableStep
  case object Load extends SkippableStep

  case class CliConfig(config: File, b64config: Boolean, include: Seq[OptionalWorkStep], skip: List[String])

  case class Config() // Contains parsed configs

  object Config {
    
  }

  val parser = new scopt.OptionParser[CliConfig]("scopt") {
    head("Relational Database Loader", ProjectMetadata.version)

    opt[File]('c', "config").required().valueName("<file>").
      action((x, c) ⇒ c.copy(config = x)).
      text("configuration file")

    opt[Unit]('b', "base64-config-string").action((_, c) ⇒
      c.copy(b64config = true)).text("base64-encoded configuration string")

    opt[Seq[OptionalWorkStep]]('i', "include").action((x, c) ⇒
      c.copy(include = x)).text("include optional work steps")

    help("help").text("prints this usage text")

  }

  println("Hello Relational Database Loader!")
}
