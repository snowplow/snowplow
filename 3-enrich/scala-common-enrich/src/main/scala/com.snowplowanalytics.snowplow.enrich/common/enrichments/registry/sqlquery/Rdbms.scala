/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry.sqlquery

import java.sql._

import scalaz._
import Scalaz._

/**
 * Common trait for all Databases
 * Contains exception-free logic wrapping JDBC to acquire DB-connection
 * and handle its lifecycle
 */
trait Rdbms {

  /**
   * Placeholder for database driver (not used)
   */
  val driver: Class[_]

  /**
   * Correctly generated connection URI specific for database
   */
  val connectionString: String

  /**
   * Cached connection, it persist until it is open. After closing getConnection
   * will try to reinitilize it
   */
  private[this] var lastConnection: ThrowableXor[Connection] =
    InvalidStateException("SQL Query Enrichment: Connection hasn't been initialized").left

  /**
   * Try to initialize new connection if cached one is closed or wasn't
   * acquired successfully
   *
   * @return successful connection if it was in cache or initialized or
   *         Throwable as failure
   */
  def getConnection: ThrowableXor[Connection] = lastConnection match {
    case \/-(c) if !c.isClosed => c.right
    case _ =>
      try { lastConnection = DriverManager.getConnection(connectionString).right } catch {
        case e: SQLException => lastConnection = e.left
      }
      lastConnection
  }

  /**
   * Execute filled PreparedStatement
   */
  def execute(preparedStatement: PreparedStatement): ThrowableXor[ResultSet] =
    try {
      preparedStatement.executeQuery().right
    } catch {
      case e: SQLException => e.left
    }

  /**
   * Get amount of placeholders (?-signs) in PreparedStatement
   */
  def getPlaceholderCount(preparedStatement: PreparedStatement): ThrowableXor[Int] =
    \/ fromTryCatch preparedStatement.getParameterMetaData.getParameterCount

  /**
   * Transform SQL-string with placeholders (?-signs) into PreparedStatement
   */
  def createEmptyStatement(sql: String): ThrowableXor[PreparedStatement] =
    for { connection <- getConnection } yield connection.prepareStatement(sql)
}

/**
 * Class representing connection configuration for databases speaking PostgreSQL dialect
 */
case class PostgresqlDb(
  host: String,
  port: Int,
  sslMode: Boolean,
  username: String,
  password: String,
  database: String
) extends Rdbms {

  val driver: Class[_] = Class.forName("org.postgresql.Driver") // Load class

  val connectionString = s"jdbc:postgresql://$host:$port/$database?user=$username&password=$password" ++ (if (sslMode)
                                                                                                            "&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
                                                                                                          else "")
}

/**
 * Class representing connection configuration for databases speaking MySQL dialect
 */
case class MysqlDb(
  host: String,
  port: Int,
  sslMode: Boolean,
  username: String,
  password: String,
  database: String
) extends Rdbms {

  val driver: Class[_] = Class.forName("com.mysql.jdbc.Driver") // Load class

  val connectionString = s"jdbc:mysql://$host:$port/$database?user=$username&password=$password" ++ (if (sslMode)
                                                                                                       "&useSsl=true&verifyServerCertificate=false"
                                                                                                     else "")
}
