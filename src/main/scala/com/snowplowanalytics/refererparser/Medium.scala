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
 * Enumeration for supported mediums.
 */
sealed abstract class Medium(val value: String)

object Medium {
  def fromString(s: String): Option[Medium] = s match {
    case UnknownMedium.value  => Some(UnknownMedium)
    case SearchMedium.value   => Some(SearchMedium)
    case InternalMedium.value => Some(InternalMedium)
    case SocialMedium.value   => Some(SocialMedium)
    case EmailMedium.value    => Some(EmailMedium)
    case PaidMedium.value     => Some(PaidMedium)
    case _                    => None
  }

  val Unknown  = UnknownMedium
  val Search   = SearchMedium
  val Internal = InternalMedium
  val Social   = SocialMedium
  val Email    = EmailMedium
  val Paid     = PaidMedium
}

case object UnknownMedium  extends Medium("unknown")
case object SearchMedium   extends Medium("search")
case object InternalMedium extends Medium("internal")
case object SocialMedium   extends Medium("social")
case object EmailMedium    extends Medium("email")
case object PaidMedium     extends Medium("paid")
