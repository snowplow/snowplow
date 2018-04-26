package com.snowplowanalytics
package snowplow
package enrich
package beam

import common.utils.JsonUtils
import iglu.client.Resolver

object SpecHelpers {

  val resolverConfig = """
    {
      "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      "data": {
        "cacheSize": 500,
        "repositories": [
          {
            "name": "Iglu Central",
            "priority": 0,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": { "http": { "uri": "http://iglucentral.com" } }
          }
        ]
      }
    }
  """

  implicit val resolver = (for {
    json <- JsonUtils.extractJson("", resolverConfig)
    resolver <- Resolver.parse(json).leftMap(_.toString)
  } yield resolver).fold(
    e => throw new RuntimeException(e),
    r => r
  )

  val enrichmentConfig = """
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
      "data": {
        "name": "anon_ip",
        "vendor": "com.snowplowanalytics.snowplow",
        "enabled": true,
        "parameters": { "anonOctets": 1 }
      }
    }
  """

}
