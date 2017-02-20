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

import cats.Functor
import cats.data._
import cats.free.Free
import cats.implicits._

// This project
import Common._
import LoaderA._
import LoaderError._
import RedshiftLoadStatements._
import Common.SqlString
import config.{ SnowplowConfig, Step }
import config.StorageTarget.RedshiftConfig


/**
 * Module containing specific for Redshift target loading
 * Works in three steps:
 * 1. Discover all data to load, including atomic and shredded
 * 2. Discover metadata and construct SQL-statements
 * 3. Load data into Redshift
 * Errors of discovering steps are accumulating
 */
object RedshiftLoader {

  /**
   * Build `LoaderA` structure to discovery data in `shredded.good`
   * and associated metadata (types, JSONPaths etc),
   * build SQL statements to load this data and perform loading.
   * Primary working method. Does not produce side-effects
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   */
  def run(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step]) = {
    for {
      statements <- discover(config, target, steps).addStep(Step.Discover)
      result <- load(statements)
    } yield result
  }

  /**
   * Perform data-loading and final statements execution in target database.
   * Should be used as last step
   *
   * @param statements prepared load statements
   * @return
   */
  def load(statements: RedshiftLoadStatements) = {
    import LoaderA._

    val loadStatements = statements.events :: statements.shredded

    for {
      _ <- executeTransaction(loadStatements).addStep(Step.Load)
      _ <- executeTransaction(List(statements.manifest)).withoutStep
      _ <- vacuum(statements)
      _ <- analyze(statements)
    } yield ()
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: RedshiftLoadStatements): TargetLoading[LoaderError, Unit] = {
    statements.analyze match {
      case Some(analyze) => executeTransaction(analyze).addStep(Step.Analyze)
      case None =>
        val noop: Action[Either[LoaderError, Unit]] = Free.pure(().asRight)
        noop.withoutStep
    }
  }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum(statements: RedshiftLoadStatements): TargetLoading[LoaderError, Long] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val block = SqlString.unsafeCoerce("END;") :: vacuum
        executeQueries(block).addStep(Step.Vacuum)
      case None =>
        val noop: Action[Either[LoaderError, Long]] = Free.pure(0L.asRight)
        noop.withoutStep
    }
  }

  /**
   * Discovers data in `shredded.good` folder with its associated metadata
   * (types, JSONPath files etc) and build SQL-statements to load it
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return action to perform all necessary S3 interactions
   */
  def discover(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step]): Discovery[RedshiftLoadStatements] = {
    val noop: ActionValidated[List[ShreddedStatements]] =
      Free.pure(Nil.validNel[DiscoveryFailure])

    val atomicCopy: Action[ValidatedNel[DiscoveryFailure, SqlString]] =
      getAtomicCopyStatements(config, target, steps).map(_.toValidatedNel)

    val loadShred: Action[ValidatedNel[DiscoveryFailure, List[ShreddedStatements]]] =
      if (steps.contains(Step.Shred)) ShreddedStatements.discover(config, target) else noop

    val loadStatementsNested = (Nested(atomicCopy) |@| Nested(loadShred)).map {
      (atomic: SqlString, shred: List[ShreddedStatements]) =>
        buildLoadStatements(target, steps, atomic, shred)
    }

    loadStatementsNested.value.map {
      case Validated.Valid(statements) => statements.asRight
      case Validated.Invalid(failures) => DiscoveryError(failures.toList).asLeft
    }
  }

  /**
   * Find remaining run id in `shredded/good` folder and build SQL statement to
   * COPY FROM this run id
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return valid SQL statement to LOAD
   */
  def getAtomicCopyStatements(config: SnowplowConfig, target: RedshiftConfig, steps: Set[Step]): DiscoveryAction[SqlString] = {
    val atomic = Common.discoverAtomic(config.aws.s3.buckets.shredded.good)
    DiscoveryAction.map(atomic) { (folder: S3.Folder) =>
      buildCopyFromTsvStatement(config, target, folder, steps)
    }
  }

  /**
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)

  private[loaders] object ShreddedStatements {

    type StatementsDiscovery = Action[List[DiscoveryStep[ShreddedStatements]]]

    private val ALDF = Functor[Action].compose[List].compose[DiscoveryStep]
    private val ALF = Functor[Action].compose[List]

    /**
     * Build groups of SQL statement for all shredded types
     * It discovers all shredded types in `shredded/good` (if `shred` step is not omitted),
     * Collects all error, either on Shredded types in discovering or JSONPaths discovering
     *
     * @param config main Snowplow configuration
     * @param target Redshift storage target configuration
     * @return either list of shredded statements or aggregated failures
     */
    def discover(config: SnowplowConfig, target: RedshiftConfig): Action[ValidatedNel[DiscoveryFailure, List[ShreddedStatements]]] = {
      val shredJobVersion = config.storage.versions.rdbShredder
      val shreddedGood = config.aws.s3.buckets.shredded.good
      val region = config.aws.s3.region
      val assets = config.aws.s3.buckets.jsonpathAssets

      val shreddedTypes =
        ShreddedType.discoverShreddedTypes(shreddedGood, shredJobVersion, region, assets).map(_.toList)

      val shreddedStatements: StatementsDiscovery =
        ALDF.map(shreddedTypes)(transformShreddedType(config, target, _))

      // Aggregate errors
      ALF.map(shreddedStatements)(_.toValidatedNel).map(_.sequence)
    }

    /**
     * Build group of SQL statements for particular shredded type
     *
     * @param config main Snowplow configuration
     * @param target Redshift storage target configuration
     * @param shreddedType full info about shredded type found in `shredded/good`
     * @return three SQL-statements to load `shreddedType` from S3
     */
    def transformShreddedType(config: SnowplowConfig, target: RedshiftConfig, shreddedType: ShreddedType): ShreddedStatements = {
      val tableName = target.shreddedTable(ShreddedType.getTableName(shreddedType))
      val copyFromJson = buildCopyFromJsonStatement(config, shreddedType.getObjectPath, shreddedType.jsonPaths, tableName, target.maxError)
      val analyze = buildAnalyzeStatement(tableName)
      val vacuum = buildVacuumStatement(tableName)
      ShreddedStatements(copyFromJson, analyze, vacuum)
    }
  }
}
