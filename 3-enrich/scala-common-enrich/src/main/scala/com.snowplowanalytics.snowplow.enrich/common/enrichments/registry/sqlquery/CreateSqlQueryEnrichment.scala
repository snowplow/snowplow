/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import cats.{Eval, Id}
import cats.effect.Sync
import cats.syntax.functor._
import cats.syntax.flatMap._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.SqlQueryConf

/** Initialize resources, necessary for SQL Query enrichment: cache and connection */
sealed trait CreateSqlQueryEnrichment[F[_]] {
  def create(conf: SqlQueryConf): F[SqlQueryEnrichment[F]]
}

object CreateSqlQueryEnrichment {

  def apply[F[_]](implicit ev: CreateSqlQueryEnrichment[F]): CreateSqlQueryEnrichment[F] = ev

  implicit def syncCreateSqlQueryEnrichment[F[_]: Sync: DbExecutor](
    implicit CLM: SqlCacheInit[F],
    CN: ConnectionRefInit[F]
  ): CreateSqlQueryEnrichment[F] =
    new CreateSqlQueryEnrichment[F] {
      def create(conf: SqlQueryConf): F[SqlQueryEnrichment[F]] =
        for {
          cache <- CLM.create(conf.cache.size)
          connection <- CN.create(1)
        } yield SqlQueryEnrichment(
          conf.schemaKey,
          conf.inputs,
          conf.db,
          conf.query,
          conf.output,
          conf.cache.ttl,
          cache,
          connection
        )
    }

  implicit def evalCreateSqlQueryEnrichment(
    implicit CLM: SqlCacheInit[Eval],
    CN: ConnectionRefInit[Eval],
    DB: DbExecutor[Eval]
  ): CreateSqlQueryEnrichment[Eval] =
    new CreateSqlQueryEnrichment[Eval] {
      def create(conf: SqlQueryConf): Eval[SqlQueryEnrichment[Eval]] =
        for {
          cache <- CLM.create(conf.cache.size)
          connection <- CN.create(1)
        } yield SqlQueryEnrichment(
          conf.schemaKey,
          conf.inputs,
          conf.db,
          conf.query,
          conf.output,
          conf.cache.ttl,
          cache,
          connection
        )
    }

  implicit def idCreateSqlQueryEnrichment(
    implicit CLM: SqlCacheInit[Id],
    CN: ConnectionRefInit[Id],
    DB: DbExecutor[Id]
  ): CreateSqlQueryEnrichment[Id] =
    new CreateSqlQueryEnrichment[Id] {
      def create(conf: SqlQueryConf): Id[SqlQueryEnrichment[Id]] =
        for {
          cache <- CLM.create(conf.cache.size)
          connection <- CN.create(1)
        } yield SqlQueryEnrichment(
          conf.schemaKey,
          conf.inputs,
          conf.db,
          conf.query,
          conf.output,
          conf.cache.ttl,
          cache,
          connection
        )
    }
}
