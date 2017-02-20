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

// This project
import Common._
import RedshiftLoader.ShreddedStatements
import config.{ SnowplowConfig, Step }
import config.StorageTarget.RedshiftConfig


/**
 * Result of discovery and SQL-statement generation steps
 *
 * @param events COPY FROM statement to load `events` table
 * @param shredded COPY FROM statements to load shredded table if necessary
 * @param vacuum VACUUM statements **including `events` table** if necessary
 * @param analyze ANALYZE statements **including `events` table** if necessary
 * @param manifest SQL statement to populate `manifest` table
 */
case class RedshiftLoadStatements(
    events: SqlString,
    shredded: List[SqlString],
    vacuum: Option[List[SqlString]],
    analyze: Option[List[SqlString]],
    manifest: SqlString)

object RedshiftLoadStatements {

  val EventFieldSeparator = "\t"

  /**
   * Constructor for `RedshiftLoadStatements`. Deconstructs discovered
   * statements and adds only those that are required based
   * on passed `steps` argument
   *
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @param atomicCopyStatements COPY statements for `events` table
   * @param shreddedStatements statements for shredded tables (include COPY,
   *                           ANALYZE and VACUUM)
   * @return statements ready to be executed on Redshift
   */
  def buildLoadStatements(
      target: RedshiftConfig,
      steps: Set[Step],
      atomicCopyStatements: SqlString,
      shreddedStatements: List[ShreddedStatements]
   ): RedshiftLoadStatements = {
    val shreddedCopyStatements = shreddedStatements.map(_.copy)

    val manifestStatement = getManifestStatements(target.schema, shreddedStatements.size)

    // Vacuum all tables including events-table
    val vacuum = if (steps.contains(Step.Vacuum)) {
      val statements = buildVacuumStatement(target.eventsTable) :: shreddedStatements.map(_.vacuum)
      Some(statements)
    } else None

    // Analyze all tables including events-table
    val analyze = if (steps.contains(Step.Analyze)) {
      val statements = buildAnalyzeStatement(target.eventsTable) :: shreddedStatements.map(_.analyze)
      Some(statements)
    } else None

    RedshiftLoadStatements(atomicCopyStatements, shreddedCopyStatements, vacuum, analyze, manifestStatement)
  }


  /**
   * Build COPY FROM TSV SQL-statement for non-shredded types and atomic.events table
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param s3path S3 path to atomic-events folder with shredded TSV files
   * @param steps SQL steps
   * @return valid SQL statement to LOAD
   */
  def buildCopyFromTsvStatement(config: SnowplowConfig, target: RedshiftConfig, s3path: S3.Folder, steps: Set[Step]): SqlString = {
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)
    val comprows = if (steps.contains(Step.Compupdate)) s"COMPUPDATE COMPROWS ${target.compRows}" else ""

    SqlString.unsafeCoerce(s"""
      |COPY ${target.eventsTable} FROM '$s3path'
      | CREDENTIALS 'aws_iam_role=${target.roleArn}' REGION AS '${config.aws.s3.region}'
      | DELIMITER '$EventFieldSeparator' MAXERROR ${target.maxError}
      | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS
      | TIMEFORMAT 'auto' ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  /**
   * Build standard manifest-table insertion
   *
   * @param databaseSchema storage target schema
   * @param shreddedCardinality number of loaded shredded types
   * @return SQL statement ready to be executed
   */
  def getManifestStatements(databaseSchema: String, shreddedCardinality: Int): SqlString = {
    val eventsTable = Common.getEventsTable(databaseSchema)

    SqlString.unsafeCoerce(s"""
                              |INSERT INTO ${Common.getManifestTable(databaseSchema)}
                              | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, $shreddedCardinality AS shredded_cardinality
                              | FROM $eventsTable
                              | WHERE etl_tstamp IS NOT null
                              | GROUP BY 1
                              | ORDER BY etl_tstamp DESC
                              | LIMIT 1;""".stripMargin)
  }

  /**
   * Build COPY FROM JSON SQL-statement for shredded types
   *
   * @param config main Snowplow configuration
   * @param s3path S3 path to folder with shredded JSON files
   * @param jsonPathsFile S3 path to JSONPath file
   * @param tableName valid Redshift table name for shredded type
   * @return valid SQL statement to LOAD
   */
  def buildCopyFromJsonStatement(config: SnowplowConfig, s3path: String, jsonPathsFile: String, tableName: String, maxError: Int, roleArn: String): SqlString = {
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)

    SqlString.unsafeCoerce(s"""
      |COPY $tableName FROM '$s3path'
      | CREDENTIALS 'aws_iam_role=$roleArn' JSON AS '$jsonPathsFile'
      | REGION AS '${config.aws.s3.region}'
      | MAXERROR $maxError TRUNCATECOLUMNS TIMEFORMAT 'auto'
      | ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  /**
   * Build ANALYZE SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid ANALYZE SQL-statement
   */
  def buildAnalyzeStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"ANALYZE $tableName;")

  /**
   * Build VACUUM SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid VACUUM SQL-statement
   */
  def buildVacuumStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"VACUUM SORT ONLY $tableName;")

  /**
<<<<<<< HEAD
=======
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  private case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)

  /**
   * Build group of SQL statements for particular shredded type
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param shreddedType full info about shredded type found in `shredded/good`
   * @return three SQL-statements to load `shreddedType` from S3
   */
  private def transformShreddedType(config: SnowplowConfig, target: RedshiftConfig, shreddedType: ShreddedType): ShreddedStatements = {
    val tableName = target.shreddedTable(ShreddedType.getTableName(shreddedType))
    val copyFromJson = buildCopyFromJsonStatement(config, shreddedType.getLoadPath, shreddedType.jsonPaths, tableName, target.maxError, target.roleArn)
    val analyze = buildAnalyzeStatement(tableName)
    val vacuum = buildVacuumStatement(tableName)
    ShreddedStatements(copyFromJson, analyze, vacuum)
  }

  /**
>>>>>>> f23db2b... To original
   * Stringify output codec to use in SQL statement
   */
  private def getCompressionFormat(outputCodec: SnowplowConfig.OutputCompression): String = outputCodec match {
    case SnowplowConfig.NoneCompression => ""
    case SnowplowConfig.GzipCompression => "GZIP"
  }
}
