package com.snowplowanalytics.snowplow.enrich.beam

import com.spotify.scio.values.SCollection

class RichSCollection[T](value: SCollection[T]) {
  def partition2(p: T => Boolean): (SCollection[T], SCollection[T]) = {
    val Seq(left, right) = value.partition(2, t => if (p(t)) 0 else 1)
    (left, right)
  }
}
