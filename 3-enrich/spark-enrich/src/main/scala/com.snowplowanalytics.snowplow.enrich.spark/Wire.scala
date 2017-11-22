object Wire {
  case class EnrichJobStarted(
    jobflowId: String,
    taskId: String,
    runId: String,
    enrichVersion: String,
    rawSizes: List[Long],
    enrichments: List[String]   // Iglu URI
  )
}