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

import scala.collection.immutable.IntMap

import Input.ExtractedValue

/**
 * Class-container for chosen DB's configuration
 * Exactly one configuration must be provided
 * @param postgresql optional container for PostgreSQL configuration
 * @param mysql optional container for MySQL configuration
 */
case class Db(postgresql: Option[PostgresqlDb] = None, mysql: Option[MysqlDb] = None) {
  private val realDb: Rdbms = (postgresql, mysql) match {
    case (Some(_), Some(_)) =>
      throw new Exception(
        "SQL Query Enrichment Configuration: db must represent either " +
          "postgresql OR mysql. Both present")
    case (None, None) =>
      throw new Exception(
        "SQL Query Enrichment Configuration: db must represent either " +
          "postgresql OR mysql. None present")
    case _ => List(postgresql, mysql).flatten.head
  }

  /**
   * Create PreparedStatement and fill all its placeholders. This function expects `placeholderMap`
   * contains exact same amount of placeholders as `sql`, otherwise it will result in error
   * downstream
   * @param sql prepared SQL statement with some unfilled placeholders (?-signs)
   * @param placeholderMap IntMap with input values
   * @return filled placeholder or error (unlikely)
   */
  def createStatement(
    sql: String,
    placeholderMap: IntMap[ExtractedValue]
  ): ThrowableXor[PreparedStatement] =
    realDb.createEmptyStatement(sql).map { preparedStatement =>
      placeholderMap.foreach {
        case (index, value) =>
          value.set(preparedStatement, index)
      }
      preparedStatement
    }

  /** Get amount of placeholders (?-signs) in Prepared Statement */
  def getPlaceholderCount(sql: String): ThrowableXor[Int] =
    realDb.createEmptyStatement(sql).flatMap(realDb.getPlaceholderCount)

  /** Execute PreparedStatement */
  def execute(preparedStatement: PreparedStatement): ThrowableXor[ResultSet] =
    realDb.execute(preparedStatement)
}
