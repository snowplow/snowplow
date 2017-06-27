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

import scala.util.control.NonFatal
import java.io.FileReader
import java.nio.file.Path
import java.sql.{Connection, SQLException}
import java.util.Properties

import org.postgresql.{Driver => PgDriver}
import org.postgresql.copy.CopyManager
import org.postgresql.jdbc.PgConnection

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}

import cats.implicits._
import LoaderError._
import config.StorageTarget
import loaders.Common.SqlString

object PgInterpreter {

  def executeTransaction(conn: Connection, queries: List[SqlString]): Either[StorageTargetError, Unit] =
    if (queries.nonEmpty) {
      val begin = SqlString.unsafeCoerce("BEGIN;")
      val commit = SqlString.unsafeCoerce("COMMIT;")
      val transaction = (begin :: queries) :+ commit
      executeQueries(conn, transaction).void
    } else Right(())

  /**
   * Execute set of SQL update-statements, combine amount of updated rows
   *
   * @param queries set of valid SQL statements in string representation
   * @return number of updated rows in case of success, first failure otherwise
   */
  def executeQueries(conn: Connection, queries: List[SqlString]): Either[StorageTargetError, Long] =
    queries.map(executeQuery(conn)).sequence.map(_.combineAll)

  /**
   * Execute a single update-statement in provided Postgres connection
   *
   * @param conn Postgres connection
   * @param sql string with valid SQL statement
   * @return number of updated rows in case of success, failure otherwise
   */
  def executeQuery(conn: Connection)(sql: SqlString): Either[StorageTargetError, Int] =
    Either.catchNonFatal {
      conn.createStatement().executeUpdate(sql)
    } leftMap {
      case NonFatal(e) => StorageTargetError(Option(e.getMessage).getOrElse(e.toString))
    }

  def setAutocommit(conn: Connection, autoCommit: Boolean): Either[LoaderError, Unit] =
    try {
      Right(conn.setAutoCommit(autoCommit))
    } catch {
      case e: SQLException => Left(StorageTargetError(e.toString))
    }


  def copyViaStdin(conn: Connection, files: List[Path], copyStatement: SqlString): Either[LoaderError, Long] = {
    val copyManager = Either.catchNonFatal {
      new CopyManager(conn.asInstanceOf[PgConnection])
    } leftMap { e => StorageTargetError(e.toString) }

    for {
      manager <- copyManager
      _ <- setAutocommit(conn, false)
      result = files.traverse(copyIn(manager, copyStatement)(_)).map(_.combineAll)
      _ = result match {
        case Left(_) => conn.rollback()
        case Right(_) => conn.commit()
      }
      _ <- setAutocommit(conn, true)
      endResult <- result
    } yield endResult
  }

  def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[LoaderError, Long] =
    try {
      Right(copyManager.copyIn(copyStatement, new FileReader(file.toFile)))
    } catch {
      case NonFatal(e) => Left(StorageTargetError(e.toString))
    }

  /**
   * Get Redshift or Postgres connection
   */
  def getConnection(target: StorageTarget): Either[LoaderError, Connection] = {
    try {
      val props = new Properties()
      props.setProperty("user", target.username)
      props.setProperty("password", target.password)
      props.setProperty("tcpKeepAlive", "true")

      target match {
        case r: StorageTarget.RedshiftConfig =>
          val url = s"jdbc:redshift://${target.host}:${target.port}/${target.database}"
          if (r.sslMode == StorageTarget.Disable) {   // "disable" and "require" are not supported
            props.setProperty("ssl", "false")         // by native Redshift JDBC Driver
          } else {                                    // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-options.html
            props.setProperty("ssl", "true")
          }
          Right(new RedshiftDriver().connect(url, props))

        case p: StorageTarget.PostgresqlConfig =>
          val url = s"jdbc:postgresql://${target.host}:${target.port}/${target.database}"
          props.setProperty("sslmode", target.sslMode.asProperty)
          Right(new PgDriver().connect(url, props))
      }
    } catch {
      case NonFatal(e) => Left(StorageTargetError(s"Problems with establishing DB connection\n${e.getMessage}"))
    }
  }
}
