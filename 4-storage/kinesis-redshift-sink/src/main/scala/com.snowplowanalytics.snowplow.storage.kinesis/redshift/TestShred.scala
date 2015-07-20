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
//    val lines = Source.fromFile("/home/denismo/Documents/WS/digdeep-snowplow/3-enrich/scala-hadoop-shred/src/test/resources/enriched_input").getLines()
    val lines = Source.fromFile("/tmp/shreder.txt").getLines()
//    val lines = Source.fromFile("/home/denismo/Documents/WS/snowplow-main/4-storage/kinesis-redshift-sink/atomic_events_060.txt").getLines()
    lines.grouped(200).foreach { window =>
      window.foreach{
        line =>
          shreder.shred(line.split("\t", -1))
      }
      shreder.finished()
    }

//    shreder.shred(.split("\t", -1))
  }
}
