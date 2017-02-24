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

import scala.collection.immutable.SortedSet
import scala.io.Source
import scala.util.control.NonFatal

import java.io.File

import cats.data.ValidatedNel
import cats.instances.list._
import cats.data.Validated._
import cats.syntax.traverse._
import cats.syntax.validated._
import cats.syntax.either._

import com.snowplowanalytics.iglu.client.Resolver

import generated.ProjectMetadata
import Utils.StringEnum

object Main extends App {

  import scopt.Read

  implicit val optionalStepRead =
    Read.reads { (Utils.fromString[OptionalWorkStep](_)).andThen(_.right.get) }

  implicit val skippableStepRead =
    Read.reads { (Utils.fromString[SkippableStep](_)).andThen(_.right.get) }

  sealed trait OptionalWorkStep extends StringEnum
  case object Compupdate extends OptionalWorkStep { def asString = "compupdate" }
  case object Vacuum extends OptionalWorkStep { def asString = "vacuum" }

  sealed trait SkippableStep extends StringEnum
  case object ArchiveEnriched extends SkippableStep { def asString = "archive_enriched" }
  case object Download extends SkippableStep { def asString = "download" }
  case object Analyze extends SkippableStep { def asString = "analyze" }
  case object Delete extends SkippableStep { def asString = "delete" }
  case object Shred extends SkippableStep { def asString = "shred" }
  case object Load extends SkippableStep { def asString = "load" }

  // TODO: this probably should not contain `File`s
  case class CliConfig(
    config: File,
    targetsDir: File,
    resolver: File,
    include: Seq[OptionalWorkStep],
    skip: Seq[SkippableStep],
    b64config: Boolean)

  case class AppConfig(
    configYaml: Config,
    b64config: Boolean,
    targets: Set[Targets.StorageTarget],
    include: SortedSet[OptionalWorkStep],
    skip: SortedSet[SkippableStep]) // Contains parsed configs

  def readFile(file: File): Either[ConfigError, String] =
    try {
      Source.fromFile(file).getLines.mkString("\n").asRight
    } catch {
      case NonFatal(e) => ParseError(s"Configuration file [${file.getAbsolutePath}] cannot be parsed").asLeft
    }

  def loadTargetsFromDir(directory: File, resolver: Resolver): ValidatedNel[ConfigError, List[Targets.StorageTarget]] = {
    if (!directory.isDirectory) ParseError(s"[${directory.getAbsolutePath}] is not a directory").invalidNel
    else if (!directory.canRead) ParseError(s"Targets directory [${directory.getAbsolutePath} is not readable").invalidNel
    else {
      val loadTarget = Targets.fooo(resolver) _
      val fileList = directory.listFiles.toList
      val targetsList = fileList.map(f => readFile(f).flatMap(loadTarget))
      targetsList.map(_.toValidatedNel).sequenceU
    }
  }

  def loadResolver(resolverConfig: File): ValidatedNel[ConfigError, Resolver] = {
    if (!resolverConfig.isFile) ParseError(s"[${resolverConfig.getAbsolutePath}] is not a file")
    else if (!resolverConfig.canRead) ParseError(s"Resolver config [${resolverConfig.getAbsolutePath} is not readable").invalidNel
    else {
      import Compat._
      val f = readFile(resolverConfig).flatMap(Targets.safeParse).toValidatedNel
      val z: ValidatedNel[ValidationError, Resolver] = f.andThen(json => Resolver.parse(json))
    }

    ???
  }

  def transform(cliConfig: CliConfig): Either[String, AppConfig] = {
    val resolver = loadResolver(cliConfig.resolver)
    val targets = loadTargetsFromDir(cliConfig.targetsDir, ???)

    // validatednel config
    // with
    // validatednel resolver or targets

//    for {
//      config <- readFile(cliConfig.config)
//
//    } ???
//    ???
    "".asLeft
  }

  val parser = new scopt.OptionParser[CliConfig]("scopt") {
    head("Relational Database Loader", ProjectMetadata.version)

    opt[File]('c', "config").required().valueName("<file>").
      action((x, c) ⇒ c.copy(config = x)).
      text("configuration file")

    opt[File]('t', "targets").required().valueName("<dir>").
      action((x, c) => c.copy(targetsDir = x)).
      text("directory with storage targets configuration JSONs")

    opt[Unit]('b', "base64-config-string").action((_, c) ⇒
      c.copy(b64config = true)).text("base64-encoded configuration string")

    opt[Seq[OptionalWorkStep]]('i', "include").action((x, c) ⇒
      c.copy(include = x)).text("include optional work steps")

    opt[Seq[SkippableStep]]('s', "skip").action((x, c) =>
      c.copy(skip = x)).text("skip steps")

    help("help").text("prints this usage text")

  }

  println("Hello Relational Database Loader!")
}
