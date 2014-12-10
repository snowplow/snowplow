 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
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
// TODO make this a package object

package com.snowplowanalytics.snowplow.storage.kinesis

// Amazon
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject

// Scalaz
import scalaz._
import Scalaz._

package object elasticsearch {

  /**
   * The original tab separated enriched event together with
   * a validated ElasticsearchObject created from it (or list of errors
   * if the creation process failed)
   * Can't use NonEmptyList as it isn't serializable
   */
  type ValidatedRecord = (String, Validation[List[String], JsonRecord])

  type EmitterInput = (String, Validation[List[String], ElasticsearchObject])
}
