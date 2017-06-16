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
package interpreters

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes

import cats._
import cats.implicits._

import com.amazonaws.services.s3.AmazonS3

import java.sql.Connection
import java.nio.file._
import java.util.Comparator

import org.postgresql.jdbc.PgConnection

import com.snowplowanalytics.snowplow.rdbloader.LoaderError.{LoaderLocalError, StorageTargetError}

import scala.util.control.NonFatal

import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import config.CliConfig
import LoaderA._
import utils.Common
import com.snowplowanalytics.snowplow.rdbloader.{ Log => ExitLog }

/**
 * Interpreter performs all actual side-effecting work,
 * interpreting `Action` at the end-of-the-world.
 * It contains and handles configuration, connections and mutable state,
 * all real-world interactions, except argument parsing
 */
class Interpreter private(
  cliConfig: CliConfig,
  dbConnection: Connection,
  amazonS3: AmazonS3,
  tracker: Option[Tracker]) {

  def run: LoaderA ~> Id = new (LoaderA ~> Id) {

    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case ListS3(folder, maxKeys) =>
          S3Interpreter.list(amazonS3, folder, maxKeys).map(S3.getKey)
        case KeyExists(key) =>
          S3Interpreter.keyExists(amazonS3, key)
        case DownloadData(source, dest) =>
          S3Interpreter.downloadData(amazonS3, source, dest)

        case ExecuteQuery(query) =>
          PgInterpreter.executeQuery(dbConnection)(query)
        case ExecuteTransaction(queries) =>
          PgInterpreter.executeTransaction(dbConnection, queries)
        case ExecuteQueries(queries) =>
          PgInterpreter.executeQueries(dbConnection, queries)
        case CopyViaStdin(files, query) =>
          PgInterpreter.copyViaStdin(dbConnection, files, query)

        case CreateTmpDir =>
          try {
            Right(Files.createTempDirectory("rdb-loader"))
          } catch {
            case NonFatal(e) => Left(LoaderLocalError("Cannot create temporary directory.\n" + e.toString))
          }
        case DeleteDir(path) =>
          Files.walkFileTree(path, Interpreter.DeleteVisitor)
          ().asInstanceOf[Id[A]]


        case Sleep(timeout) =>
          Thread.sleep(timeout)
        case Track(result) =>
          result match {
            case ExitLog.LoadingSucceeded(_) =>
              TrackerInterpreter.trackSuccess(tracker)
            case ExitLog.LoadingFailed(message, _) =>
              val sanitizedMessage = Common.sanitize(message, List(cliConfig.target.password, cliConfig.target.username))
              TrackerInterpreter.trackError(tracker, sanitizedMessage)
          }
        case Dump(result) =>
          TrackerInterpreter.dumpStdout(amazonS3, cliConfig.logKey, result.toString)
        case Exit(loadResult, dumpResult) =>
          dbConnection.close()
          TrackerInterpreter.exit(loadResult, dumpResult)
      }
    }
  }
}

object Interpreter {

  object DeleteVisitor extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes) = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      Files.delete(dir)
      FileVisitResult.CONTINUE
    }
  }

  /**
   * Initialize clients/connections for interpreter and interpreter itself
   *
   * @param cliConfig RDB Loader app configuration
   * @return prepared interpreter
   */
  def initialize(cliConfig: CliConfig): Interpreter = {

    val dbConnection = PgInterpreter.getConnection(cliConfig.target)
    val amazonS3 = S3Interpreter.getClient(cliConfig.configYaml.aws)
    val tracker = TrackerInterpreter.initializeTracking(cliConfig.configYaml.monitoring)

    new Interpreter(cliConfig, dbConnection, amazonS3, tracker)
  }
}
