/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.RejectionHandler
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives.complete
import akka.stream.ActorMaterializer
import com.typesafe.config.{ConfigFactory, Config}
import org.slf4j.LoggerFactory
import pureconfig._

import model._

// Main entry point of the Scala collector.
trait Collector {

  lazy val log = LoggerFactory.getLogger(getClass())

  def parseConfig(args: Array[String]): (CollectorConfig, Config) = {
    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig](generated.BuildInfo.name) {
      head(generated.BuildInfo.name, generated.BuildInfo.version)
      help("help")
      version("version")
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
    }

    val conf = parser.parse(args, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }

    if (!conf.hasPath("collector")) {
      System.err.println("configuration has no \"collector\" path")
      System.exit(1)
    }

    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    implicit val sinkConfigHint = new FieldCoproductHint[SinkConfig]("enabled")
    (loadConfigOrThrow[CollectorConfig](conf.getConfig("collector")), conf)
  }

  def run(collectorConf: CollectorConfig, akkaConf: Config, sinks: CollectorSinks): Unit = {

    implicit val system = ActorSystem.create("scala-stream-collector", akkaConf)
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher
    implicit val rejectionHandler = RejectionHandler.newBuilder()
      .handle { case DoNotTrackRejection =>
        complete(HttpResponse(entity = "do not track"))
      }
      .result()

    val route = new CollectorRoute {
      override def collectorService = new CollectorService(collectorConf, sinks)
    }

    Http().bindAndHandle(route.collectorRoute, collectorConf.interface, collectorConf.port)
      .map { binding =>
        log.info(s"REST interface bound to ${binding.localAddress}")
      } recover { case ex =>
        log.error("REST interface could not be bound to " +
          s"${collectorConf.interface}:${collectorConf.port}", ex.getMessage)
      }
  }
}
