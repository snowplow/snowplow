/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam

import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection

class RichSCollectionSpec extends PipelineSpec {

  implicit def enrichSCollection[T](collection: SCollection[T]) = new RichSCollection[T](collection)

  "RichSCollection" should "support partition(predicate)" in {
    runWithContext { sc =>
      val (p1, p2) = sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).partition2(_ % 2 == 0)
      p1 should containInAnyOrder (Seq(2, 4, 6))
      p2 should containInAnyOrder (Seq(1, 3, 5))
    }
  }

}
