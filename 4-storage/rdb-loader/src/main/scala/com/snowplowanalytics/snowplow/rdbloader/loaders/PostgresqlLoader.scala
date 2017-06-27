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
import cats.free.Free

// This project
import LoaderA._
import config.{Step, SnowplowConfig}
import config.StorageTarget.PostgresqlConfig

object PostgresqlLoader {

  /**
   * Discovery data in `shredded.good`, build SQL statements to
   * load this data and build `LoaderA` structure to interpret.
   * Primary working method. Does not produce side-effects
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   */
  def run(config: SnowplowConfig, target: PostgresqlConfig, steps: Set[Step]) = {
    val shreddedGood = config.aws.s3.buckets.shredded.good
    val discovery = DataDiscovery.discoverAtomic(shreddedGood)
    val statements = PostgresqlLoadStatements.build(target.eventsTable, steps)

    for {
      folders <- discovery.addStep(Step.Discover)
      _ <- folders.traverse(loadFolder(statements))
      _ <- analyze(statements)
      _ <- vacuum(statements)
    } yield ()
  }

  /**
   * Load and cleanup single folder
   *
   * @param statement PostgreSQL atomic.events load statements
   * @param discovery discovered run folder
   * @return changed app state
   */
  def loadFolder(statement: PostgresqlLoadStatements)(discovery: DataDiscovery): TargetLoading[LoaderError, Long] = {
    for {
      tmpdir <- createTmpDir.withoutStep
      files  <- downloadData(discovery.atomicEvents, tmpdir).addStep(Step.Download)
      count  <- copyViaStdin(files, statement.events).addStep(Step.Load)
      _      <- deleteDir(tmpdir).addStep(Step.Delete)
    } yield count
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze(statements: PostgresqlLoadStatements): TargetLoading[LoaderError, Unit] = {
    statements.analyze match {
      case Some(analyze) =>
        val result = executeQueries(List(analyze)).map(_.void)
        result.addStep(Step.Analyze)
      case None =>
        val noop: Action[Either[LoaderError, Unit]] = Free.pure(Right(()))
        noop.withoutStep
    }
  }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum(statements: PostgresqlLoadStatements): TargetLoading[LoaderError, Unit] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val result = executeQueries(List(vacuum)).map(_.void)
        result.addStep(Step.Vacuum)
      case None =>
        val noop: Action[Either[LoaderError, Unit]] = Free.pure(Right(()))
        noop.withoutStep
    }
  }

}
