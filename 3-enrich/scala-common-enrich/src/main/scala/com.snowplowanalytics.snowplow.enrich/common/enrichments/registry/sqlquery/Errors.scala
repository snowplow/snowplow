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

sealed trait SqlQueryEnrichmentError extends Throwable {
  val message: String
  override def toString = message
  override def getMessage = message
}
final case class ValueNotFoundException(message: String) extends SqlQueryEnrichmentError
final case class JsonPathException(message: String) extends SqlQueryEnrichmentError
final case class InvalidStateException(message: String) extends SqlQueryEnrichmentError
final case class InvalidConfiguration(message: String) extends SqlQueryEnrichmentError
final case class InvalidDbResponse(message: String) extends SqlQueryEnrichmentError
final case class InvalidInput(message: String) extends SqlQueryEnrichmentError
