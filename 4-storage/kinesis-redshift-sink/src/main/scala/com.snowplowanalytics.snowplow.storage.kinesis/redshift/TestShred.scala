package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.snowplowanalytics.iglu.client.Resolver
import org.postgresql.ds.PGPoolingDataSource

import scala.language.implicitConversions
import scala.reflect.io.File
import scalaz.{Failure, Success}

/**
 * Created by denismo on 17/07/15.
 */
object TestShred {
  def main(args: Array[String]) {
    val source: PGPoolingDataSource = new PGPoolingDataSource
    source.setUrl("jdbc:postgresql://snowplow.digdeep.digdeepdigital.com.au:5439/snowplow")
    source.setDataSourceName("Postgre Test")
    source.setUser("admin")
    source.setPassword("TJIbUnNP398GmmpG9C7o")
    source.setMaxConnections(10)
    val Mapper = new ObjectMapper
    implicit val igluResolver:Resolver = Resolver.parse(Mapper.readTree("{\n                   \"schema\": \"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0\",\n                   \"data\": {\n                     \"cacheSize\": 500,\n                     \"repositories\": [\n                       {\n                         \"name\": \"Iglu Central\",\n                         \"priority\": 0,\n                         \"vendorPrefixes\": [\n                           \"com.snowplowanalytics\"\n                         ],\n                         \"connection\": {\n                           \"http\": {\n                             \"uri\": \"http://iglucentral.com\"\n                           }\n                         }\n                       },\n                       {\n                         \"name\": \"Digdeep\",\n                         \"priority\": 0,\n                         \"vendorPrefixes\": [\n                           \"com.au.digdeep\"\n                         ],\n                         \"connection\": {\n                           \"http\": {\n                             \"uri\": \"http://digdeep-snowplow-hosted-assets.s3-website-ap-southeast-2.amazonaws.com\"\n                           }\n                         }\n                       }\n                     ]\n                   }\n               }")) match {
      case Success(s) => s
      case Failure(f) => throw new RuntimeException("Must provide iglu_config: " + f)
    }
    implicit val props = new Properties()
    props.setProperty("defaultSchema", "test")

    val shreder = new InstantShreder(source)
    import scala.io.Source
    val lines = Source.fromFile("/home/denismo/Documents/WS/digdeep-snowplow/3-enrich/scala-hadoop-shred/src/test/resources/enriched_input").getLines()
    lines.sliding(200).foreach { window =>
      window.foreach(line => shreder.shred(line.split("\t", -1)))
      shreder.finished()
    }

//      shreder.shred("""cadreon_poc	app	2015-06-23 15:01:31.087	2015-06-23 10:01:03.000		unstruct	1b919b62-24f2-47f8-91e7-1141b8a93808			com.snowplowanalytics.iglu-v1	clj-0.9.1-tom-0.1.0	hadoop-0.12.0-common-0.11.0-SNAPSHOT		60.230.194.73				d952e18e-03c3-45e6-bd7f-d1de7f2bc27d	AU	08	Perth	6000	-31.952194	115.86139	Western Australia					http://www.bbc.com/news/health-33223045			http	www.bbc.com	80	/news/health-33223045																							{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.au.digdeep/cadreon/jsonschema/1-0-0","data":{"tp_publisher_name":"MNET AU 3","dd_device_type":"Desktop","tp_campaign_name":"AU_Industry Super_EOFY Maintenance_May-Aug 2015_INUEOF","dd_is_mobile":"false","dd_web_traffic_tor":"false","dd_device_id":"f1ba42daa6afc8b38b3fea7a2555c014ff89bf69a8eeb48cb9a8d6a09178e18fcbbc098b82d5bad798b4684f7defefb0","tp_section_name":"ROS","event_type":"impression","tp_placement_name":"MNET 3_CPM RON FEMALE_Tablet_728x90_Stdbnr","dd_web_traffic_bot":"false","tp_user_id":"12744064-63e1-4373-989d-9269b68035e5"}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.124 Safari/537.36	Chrome	Chrome	43.0.2357.124	Browser	WEBKIT															Mac OS X	Mac OS X	Apple Inc.		Computer	0					""".split("\t", -1))
  }
}
