/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream

import java.util.UUID
import java.util.concurrent.TimeUnit

import cats.Id
import cats.effect.Clock
import cats.syntax.either._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.scalatracker.UUIDProvider

object utils {
  def emitPii(enrichmentRegistry: EnrichmentRegistry[Id]): Boolean =
    enrichmentRegistry.piiPseudonymizer
      .map(_.emitIdentificationEvent)
      .getOrElse(false)

  def validatePii(emitPii: Boolean, streamName: Option[String]): Either[String, Unit] =
    (emitPii, streamName) match {
      case (true, None) => "PII was configured to emit, but no PII stream name was given".asLeft
      case _ => ().asRight
    }

  implicit val clockProvider: Clock[Id] = new Clock[Id] {
    final def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    final def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }

  implicit val uuidProvider: UUIDProvider[Id] = new UUIDProvider[Id] {
    override def generateUUID: Id[UUID] = UUID.randomUUID()
  }
}
