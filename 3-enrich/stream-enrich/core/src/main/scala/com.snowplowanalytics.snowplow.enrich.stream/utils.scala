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

import scala.util.{Failure, Success, Try}

import scalaz._
import Scalaz._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

object utils {
  // to rm once 2.12 as well as the right projections
  def fold[A, B](t: Try[A])(ft: Throwable => B, fa: A => B): B = t match {
    case Success(a) => fa(a)
    case Failure(t) => ft(t)
  }

  def toEither[A](t: Try[A]): \/[Throwable, A] = fold(t)(_.left, _.right)

  def filterOrElse[L, R](e: \/[L, R])(p: R => Boolean, l: => L): \/[L, R] = e match {
    case \/-(r) if p(r) => r.right
    case \/-(_) => l.left
    case o => o
  }

  def emitPii(enrichmentRegistry: EnrichmentRegistry): Boolean =
    enrichmentRegistry.getPiiPseudonymizerEnrichment
      .map(_.emitIdentificationEvent)
      .getOrElse(false)

  def validatePii(emitPii: Boolean, streamName: Option[String]): \/[String, Unit] =
    (emitPii, streamName) match {
      case (true, None) => "PII was configured to emit, but no PII stream name was given".left
      case _ => ().right
    }
}
