/**
 * Copyright 2012-2019 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.snowplowanalytics.refererparser

/**
 * Referer - returned from parse, a sealed hierarchy which can be an
 *  UnknownReferer, SearchReferer, InternalReferer, SocialReferer, EmailReferer,
 *  or PaidReferer.
 */
sealed trait Referer
case object UnknownReferer                                           extends Referer
final case class SearchReferer(source: String, term: Option[String]) extends Referer
case object InternalReferer                                          extends Referer
final case class SocialReferer(source: String)                       extends Referer
final case class EmailReferer(source: String)                        extends Referer
final case class PaidReferer(source: String)                         extends Referer
