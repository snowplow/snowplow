package com.snowplowanalytics.snowplow.enrich.spark

import scalaz._
import Scalaz._
import com.snowplowanalytics.snowplow.scalatracker.{Ec2Metadata, SelfDescribingJson, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.SyncEmitter
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JString
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.snowplowanalytics.iglu.client.ValidatedNel
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._


object Wire {

  case class CollectorConfig(host: String, port: Int) {
    def asString: String = s"$host:$port"
  }

  class WireClient(tracker: Tracker, emr: AmazonElasticMapReduce, cw: AmazonCloudWatch) {
    def getEnrichJobStarted(): EnrichJobStarted = {
      emr
      ???
    }

    def sendStarted(): Unit = {
      tracker.enableEc2Context()
      getEnrichJobStarted()
      tracker.trackSelfDescribingEvent(SelfDescribingJson(sparkEnrichStartedSchema, ???))

      ???
    }
  }

  val sparkEnrichStartedSchema = "iglu:com.snowplowanalytics.wire/spark_enrich_ended/jsonschema/1-0-0"

  def getClients(collectorConfig: CollectorConfig): WireClient = {
    val emitter = new SyncEmitter(collectorConfig.host, port = collectorConfig.port)
    val tracker = new Tracker(List(emitter), "mytrackername", "myapplicationid")

    val context = Ec2Metadata.getInstanceContextBlocking

    val region = context match {
      case Some(ctx) => ctx.toJObject \ "data" \ "region" match {
        case JString(r) => r
        case _ => throw new RuntimeException("cannot extract region from instance identity")
      }
      case None => throw new RuntimeException("cannot get instance identity")
    }

    val credentials = new AWSCredentialsProviderChain()
    val emrClient = AmazonElasticMapReduceClientBuilder.standard().withRegion(region).withCredentials(credentials).build()
    val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(credentials).build()

    new WireClient()(tracker, emrClient, cwClient)
  }

  case class Point(timestamp: Int, x: Long)

  case class Performance(
    memoryAllocated: List[Point],
    memoryAvailable: List[Point]
  )

  case class EnrichJobStarted(
    jobflowId: String,
    taskId: String,
    runId: String,
    enrichVersion: String,
    rawSizes: List[Long],
    enrichments: List[String]   // Iglu URI
  )

  case class EnrichJobFinished(
    jobflowId: String,
    taskId: String,
    runId: String,
    enrichVersion: String,
    enrichments: List[String],   // Iglu URI

    goodSizes: List[Long],       // Files
    badSizes: List[Long],        // Files

    goodCount: Option[Long],
    badCount: Option[Long],

    performance: Option[Performance]
  )


  def extractCollector(s: String): ValidatedNel[CollectorConfig] = s.split(":").toList match {
    case List(host, port) if port.forall(_.isDigit) => CollectorConfig(host, port.toInt).successNel
    case _ => s"$s doesn't match host:port format".toProcessingMessage.failureNel
  }

  def extractUnsafe(s: String): CollectorConfig = s.split(":").toList match {
    case List(host, port) if port.forall(_.isDigit) => CollectorConfig(host, port.toInt)
    case _ => throw new RuntimeException("Invalid collector format")
  }

}