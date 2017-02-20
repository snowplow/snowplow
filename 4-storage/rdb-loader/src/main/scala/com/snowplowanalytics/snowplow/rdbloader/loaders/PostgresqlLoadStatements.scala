/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

import Common.SqlString
import DataDiscovery.AtomicDiscovery
import config.Step

/**
 * End-of-discovery data for PostgreSQL.
 * Contains all SQL-statements need to be executed
 *
 * @param events
 * @param analyze
 * @param vacuum
 */
case class PostgresqlLoadStatements(
  events: SqlString,
  analyze: Option[SqlString],
  vacuum: Option[SqlString])

object PostgresqlLoadStatements {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  /**
   * Build `PostgresqlLoadStatements`
   */
  def build(eventsTable: String, steps: Set[Step]): PostgresqlLoadStatements = {
    val loadStatements = buildLoadStatements(eventsTable)
    val vacuum = if (steps.contains(Step.Vacuum)) Some(buildVacuumStatement(eventsTable)) else None
    val analyze = if (steps.contains(Step.Analyze)) Some(buildAnalyzeStatement(eventsTable)) else None

    PostgresqlLoadStatements(loadStatements, analyze, vacuum)
  }

  /**
   * Build COPY FROM SQL-statements to be used with stdin copy
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid COPY FROM Postgres SQL-statement
   */
  def buildLoadStatements(tableName: String): SqlString =
    Common.SqlString.unsafeCoerce(s"""
      |COPY $tableName FROM STDIN
      | WITH CSV ESCAPE E'$EscapeChar' QUOTE E'$QuoteChar'
      | DELIMITER '$EventFieldSeparator'
      | NULL '$NullString';""".stripMargin)

  /**
   * Build VACUUM SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid VACUUM SQL-statement
   */
  def buildVacuumStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"VACUUM $tableName;")

  /**
   * Build ANALYZE SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid ANALYZE SQL-statement
   */
  def buildAnalyzeStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"ANALYZE $tableName;")
}
