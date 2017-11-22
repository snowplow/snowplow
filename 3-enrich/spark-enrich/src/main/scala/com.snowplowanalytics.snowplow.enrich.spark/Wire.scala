object Wire {
  case class EnrichJobStarted(
    jobflowId: String,
    enrichVersion: String,
    runId: String,
    rawSizes: List[Int]
  )
}