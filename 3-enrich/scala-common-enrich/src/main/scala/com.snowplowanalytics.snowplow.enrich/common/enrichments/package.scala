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

import cats.data.ValidatedNel

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import io.circe.Json

package object enrichments {

  /** A context, potentially added by an enrichment or single failure */
  type EnrichContext =
    Either[FailureDetails.EnrichmentStageIssue, Option[SelfDescribingData[Json]]]

  /** Contexts, potentially added by an enrichment or list of aggregated failures */
  type EnrichContextsComplex =
    ValidatedNel[FailureDetails.EnrichmentStageIssue, List[SelfDescribingData[Json]]]

}
