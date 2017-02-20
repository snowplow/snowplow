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
package loaders

import cats.implicits._

import shapeless.tag
import shapeless.tag._

// This project
import config.CliConfig
import config.StorageTarget.{ PostgresqlConfig, RedshiftConfig }
import LoaderError.{AtomicDiscoveryFailure, DiscoveryError, DiscoveryFailure}


object Common {

  /**
   * Main "atomic" table name
   */
  val EventsTable = "events"

  /**
   * Table for load manifests
   */
  val ManifestTable = "manifest"

  /**
   * Correctly merge database schema and table name
   */
  def getEventsTable(databaseSchema: String): String =
    if (databaseSchema.isEmpty) EventsTable
    else databaseSchema + "." + EventsTable

  /**
   * Correctly merge database schema and table name
   */
  def getManifestTable(databaseSchema: String): String =
    if (databaseSchema.isEmpty) ManifestTable
    else databaseSchema + "." + ManifestTable

  /**
   * Subpath to check `atomic-events` directory presence
   */
  val atomicSubpathPattern = "(.*)/(run=[0-9]{4}-[0-1][0-9]-[0-3][0-9]-[0-2][0-9]-[0-6][0-9]-[0-6][0-9]/atomic-events)/(.*)".r
  //                                   year     month      day        hour       minute     second

  /**
   * Process any valid storage target
   *
   * @param cliConfig RDB Loader app configuration
   */
  def load(cliConfig: CliConfig): TargetLoading[LoaderError, Unit] = {
    cliConfig.target match {
      case postgresqlTarget: PostgresqlConfig =>
        PostgresqlLoader.run(cliConfig.configYaml, postgresqlTarget, cliConfig.steps)
      case redshiftTarget: RedshiftConfig =>
        RedshiftLoader.run(cliConfig.configYaml, redshiftTarget, cliConfig.steps)
    }
  }

  /**
   * Find S3 folder with atomic events
   *
   * @param shreddedGood path to shredded good directory
   * @return `LoaderA` structure to perform search
   */
  def discoverAtomic(shreddedGood: S3.Folder): DiscoveryAction[S3.Folder] =
    LoaderA.listS3(shreddedGood, 10).map { keys =>
      keys.collectFirst(Function.unlift(S3.getAtomicPath)) match {
        case Some(path) => path.asRight
        case None => AtomicDiscoveryFailure(shreddedGood).asLeft
      }
    }

  /**
   * Lift single discovery step (with possible `DiscoveryFailure`)
   * to
   *
   * @param discoveryStep
   * @tparam A
   * @return
   */
  def liftToError[A](discoveryStep: Either[DiscoveryFailure, A]): Either[DiscoveryError, A] =
    discoveryStep match {
      case Left(error) => DiscoveryError(List(error)).asLeft
      case Right(success) => success.asRight
    }


  /**
   * String representing valid SQL query/statement,
   * ready to be executed
   */
  type SqlString = String @@ SqlStringTag

  object SqlString extends tag.Tagger[SqlStringTag] {
    def unsafeCoerce(s: String) = apply(s)
  }

  sealed trait SqlStringTag
}
