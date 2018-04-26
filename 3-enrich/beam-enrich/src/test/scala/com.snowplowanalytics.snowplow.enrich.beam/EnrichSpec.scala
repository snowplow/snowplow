package com.snowplowanalytics.snowplow.enrich.beam

import java.nio.file.Paths

import com.spotify.scio.testing._

class EnrichSpec extends PipelineSpec {

  val inData = Seq(Array[Byte](97, 98, 99))
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  val r = getClass.getResource("/iglu_resolver.json")
  println(r)
  println(r.toURI)
  println(Paths.get(r.toURI()))

  "Enrich" should "work" in {
    JobTest[Enrich.type]
      .args("--input=in", "--output=out", "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()))
      .input(PubsubIO("in"), inData)
      .output(PubsubIO[String]("out"))(_ should containInAnyOrder (expected))
      .output(PubsubIO[String]("bad"))(_ should containInAnyOrder (expected))
      .run()
  }

}
