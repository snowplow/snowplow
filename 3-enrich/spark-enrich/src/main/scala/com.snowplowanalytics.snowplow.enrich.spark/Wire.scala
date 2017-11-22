package com.snowplowanalytics.snowplow.enrich.spark

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder

object Wire {


  val emrClient = AmazonElasticMapReduceClientBuilder.defaultClient()
  val cwClient = AmazonCloudWatchClientBuilder.defaultClient()

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

  def getEnrichJobStarted(): EnrichJobStarted = {

  }
}