/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package shredder

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Common Enrich
import common._
import outputs.CanonicalOutput

// This project
import hadoop.utils.JsonUtils

/**
 * The shredder takes the two fields containing JSONs
 * (contexts and unstructured event properties) and
 * "shreds" their contents into a List of JsonNodes
 * ready for loading into dedicated tables in the
 * database.
 */
object Shredder {

  def shred(event: CanonicalOutput): ValidatedShreddedJsons = {

    val contexts = Option(event.contexts).map(str =>
      JsonUtils.extractJson("context", str))
    val ueProperties = Option(event.ue_properties).map(str =>
      JsonUtils.extractJson("ue_properties", str))


    JsonUtils.extractJson("todo", "[]").leftMap(e => NonEmptyList(JsonUtils.extractJson("err", e).toOption.get)).map(j => List(j))
  }


}
