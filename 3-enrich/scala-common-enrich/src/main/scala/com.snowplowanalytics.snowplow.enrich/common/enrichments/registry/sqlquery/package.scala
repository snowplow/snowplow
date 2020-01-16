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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.sql.Connection

import scala.collection.immutable.IntMap

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.lrumap.{CreateLruMap, LruMap}

import io.circe.Json

package object sqlquery {
  type SqlCacheInit[F[_]] =
    CreateLruMap[F, IntMap[Input.ExtractedValue], (Either[Throwable, List[SelfDescribingData[Json]]], Long)]

  type SqlCache[F[_]] =
    LruMap[F, IntMap[Input.ExtractedValue], (Either[Throwable, List[SelfDescribingData[Json]]], Long)]

  type ConnectionRefInit[F[_]] = CreateLruMap[F, Unit, Either[Throwable, Connection]]

  type ConnectionRef[F[_]] = LruMap[F, Unit, Either[Throwable, Connection]]

}
