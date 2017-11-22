object Wire {
  case class EnrichJobStarted(
    jobflowId: String,
    enrichVersion: String,
    taskId: String,
    runId: String,
    rawSizes: List[Int]
  )
}