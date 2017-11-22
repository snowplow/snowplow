package com.snowplowanalytics.snowplow.enrich.spark

import scalaz._
import Scalaz._
import com.snowplowanalytics.snowplow.scalatracker.{Ec2Metadata, SelfDescribingJson, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.SyncEmitter

import org.json4s.JsonDSL._

import org.json4s.JsonAST.{JObject, JString}
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.elasticmapreduce.model.{ClusterState, ClusterSummary, DescribeClusterRequest, ListClustersRequest}
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.snowplowanalytics.iglu.client.ValidatedNel
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

import scala.annotation.tailrec
import scala.collection.convert.decorateAsScala._

object Wire {

  val sparkEnrichStartedSchema = "iglu:com.snowplowanalytics.wire/spark_enrich_started/jsonschema/1-0-0"

  class WireClient(tracker: Tracker, emr: AmazonElasticMapReduce, cw: AmazonCloudWatch, jobName: String) {

    def getActiveEmrClusterSummary: Validation[String, ClusterSummary] = {

      @tailrec
      def findSingleMatchingEmrCluster(listSoFar: List[ClusterSummary], listClustersRequest: ListClustersRequest): Validation[String, ClusterSummary] = {
        val (currentList, maybeMarker) = filterListOfEmrClusters(jobName, listClustersRequest)
        val clusterList = listSoFar ::: currentList
        (clusterList, maybeMarker) match {
          case (Nil, None) => s"Failed to discover EMR Cluster: no active clusters matched '${jobName}'".failure
          case (list, _) if list.size > 1 => s"Failed to discover unique EMR Cluster: Found more than 1 cluster matching '${jobName}'".failure
          case (_, Some(marker)) => findSingleMatchingEmrCluster(clusterList, listClustersRequest.withMarker(marker))
          case (list, None) => list.head.success
        }
      }

      try {
        val listClustersRequest = new ListClustersRequest().withClusterStates(ClusterState.RUNNING, ClusterState.WAITING)
        findSingleMatchingEmrCluster(Nil, listClustersRequest)
      } catch {
        case e: Exception => s"Could not get active EMR Clusters: ${e.getMessage}".failure
      }
    }

    private def filterListOfEmrClusters(jobName: String, listClustersRequest: ListClustersRequest): (List[ClusterSummary], Option[String]) = {
      val clusterResult = emr.listClusters(listClustersRequest)
      val resultMarker = clusterResult.getMarker
      val resultList =
        for {
          cluster <- clusterResult.getClusters.asScala.toList
          if cluster.getName.equals(jobName)
        } yield { cluster }
      (resultList, Option(resultMarker))
    }

    def getEnrichJobStarted(runId: String): EnrichJobStarted = {
      val clusterSummary = getActiveEmrClusterSummary.toOption.get
      EnrichJobStarted(clusterSummary.getId, runId, generated.ProjectSettings.version, List(), List())
    }

    def sendStarted(badRows: String): Unit = {
      tracker.enableEc2Context()
      val event = getEnrichJobStarted(badRows.split("/").last)
      tracker.trackSelfDescribingEvent(toJson(event))
    }
  }

  def toJson(enrichJobStarted: EnrichJobStarted): SelfDescribingJson = {
    val data: JObject =
      ("jobflowId" -> enrichJobStarted.jobflowId) ~
        ("runId" -> enrichJobStarted.runId) ~
        ("enrichVersion" -> enrichJobStarted.enrichVersion) ~
        ("rawSizes" -> enrichJobStarted.rawSizes) ~
        ("enrichments" -> enrichJobStarted.enrichments)
    SelfDescribingJson(sparkEnrichStartedSchema, data)
  }

  def getClients(collectorConfig: CollectorConfig, jobName: String): WireClient = {
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

    new WireClient(tracker, emrClient, cwClient, jobName)
  }

  case class CollectorConfig(host: String, port: Int) {
    def asString: String = s"$host:$port"
  }

  case class Point(timestamp: Int, x: Long)

  case class Performance(
    memoryAllocated: List[Point],
    memoryAvailable: List[Point]
  )

  case class EnrichJobStarted(
    jobflowId: String,
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