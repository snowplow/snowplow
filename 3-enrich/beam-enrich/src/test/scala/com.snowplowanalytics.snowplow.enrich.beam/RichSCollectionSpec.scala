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
