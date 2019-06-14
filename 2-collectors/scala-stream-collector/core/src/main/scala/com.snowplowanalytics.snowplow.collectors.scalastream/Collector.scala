/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import org.slf4j.LoggerFactory
import pureconfig._

import metrics._
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

    val collectorRoute = new CollectorRoute {
      override def collectorService = new CollectorService(collectorConf, sinks)
    }

    val prometheusMetricsService = new PrometheusMetricsService(collectorConf.prometheusMetrics)

    val metricsRoute = new MetricsRoute {
      override def metricsService: MetricsService = prometheusMetricsService
    }

    val metricsDirectives = new MetricsDirectives {
      override def metricsService: MetricsService = prometheusMetricsService
    }

    val routes =
      if (collectorConf.prometheusMetrics.enabled)
        metricsRoute.metricsRoute ~ metricsDirectives.logRequest(collectorRoute.collectorRoute)
      else collectorRoute.collectorRoute

    lazy val redirectRoutes =
      scheme("http") {
        extract(_.request.uri) { uri =>
          redirect(
            uri.copy(scheme = "https").withPort(collectorConf.ssl.port),
            StatusCodes.MovedPermanently
          )
        }
      }

    def bind(
        rs: Route,
        interface: String,
        port: Int,
        connectionContext: ConnectionContext = ConnectionContext.noEncryption()
    ) =
      Http().bindAndHandle(rs, interface, port, connectionContext)
        .map { binding =>
          log.info(s"REST interface bound to ${binding.localAddress}")
        } recover { case ex =>
          log.error( "REST interface could not be bound to " +
            s"${collectorConf.interface}:${collectorConf.port}", ex.getMessage)
        }

    lazy val secureEndpoint =
      bind(routes,
           collectorConf.interface,
           collectorConf.ssl.port,
           SSLConfig.secureConnectionContext(system, AkkaSSLConfig())
      )

    lazy val unsecureEndpoint = (routes: Route) =>
      bind(routes, collectorConf.interface, collectorConf.port)

    collectorConf.ssl match {
      case SSLConfig(true, true, port) =>
        unsecureEndpoint(redirectRoutes)
        secureEndpoint
      case SSLConfig(true, false, port) =>
        unsecureEndpoint(routes)
        secureEndpoint
      case _ =>
        unsecureEndpoint(routes)
    }
  }
}
