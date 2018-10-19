package com.snowplowanalytics.snowplowbadrows

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.repositories._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CljTomcatLoader
import com.snowplowanalytics.snowplow.enrich.common.loaders.CloudfrontLoader
import com.snowplowanalytics.snowplow.enrich.common.{EtlPipeline, ValidatedEnrichedEvent}
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.snowplow.Tp2Adapter
import org.joda.time.DateTime

object Enrich {
  val resolver = Resolver(
    10,
    HttpRepositoryRef(RepositoryRefConfig("Iglu Central", 1, List("com.snowplow")), "http://iglucentral.com", None))
  val enrichmentRegistry = EnrichmentRegistry(Map.empty)
  val now                = DateTime.now()

//  val clf = CloudfrontLoader.toCollectorPayload

  def parse(line: String): List[ValidatedEnrichedEvent] = {
    val payload = CljTomcatLoader.toCollectorPayload(line)
    val events  = EtlPipeline.processEvents(enrichmentRegistry, "badrows-test", now, payload)(resolver)
    events
  }

}
