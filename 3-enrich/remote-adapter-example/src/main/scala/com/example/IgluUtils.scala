package com.example

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils

import scala.io.Source

object IgluUtils {

  def getResolver(confFilename: String): Resolver = {
    val source = Source.fromFile(confFilename, "utf-8")
    val confText = try source.mkString
    finally source.close()
    (for {
      json <- JsonUtils.extractJson(confFilename, confText)
      reso <- Resolver.parse(json)
    } yield reso).getOrElse(throw new RuntimeException("Could not build an Iglu resolver"))
  }

}
