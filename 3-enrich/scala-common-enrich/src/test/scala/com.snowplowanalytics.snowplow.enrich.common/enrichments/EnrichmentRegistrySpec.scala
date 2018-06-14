package com.snowplowanalytics.snowplow.enrich
package common
package enrichments

import java.net.URI

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.Enrichment
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

case class NoFileEnrichment() extends Enrichment
case class FileEnrichment(files: List[(URI, String)]) extends Enrichment {
  override def filesToCache(): List[(URI, String)] = files
}

class EnrichmentRegistrySpec extends Specification with DataTables with ValidationMatchers {
  def is = s2"""
    Should report files to cache for all registered enrichments $e1
  """

  val files1 = makeFiles("file1_1", "file1_2")
  val files2 = makeFiles("file2")

  def makeFiles(files: String*): List[(URI, String)] =
    files.toList.map(f => (new URI(s"http://foobar.com/$f"), f))

  val nofiles     = NoFileEnrichment()
  val enrichment1 = FileEnrichment(files1)
  val enrichment2 = FileEnrichment(files2)

  def e1 =
    "SPEC NAME"             || "ENRICHMENTS"                                  | "EXPECTED FILES" |
      "none with files"     !! enrichments(nofiles, nofiles, nofiles)         ! List.empty |
      "one with files"      !! enrichments(nofiles, enrichment1, nofiles)     ! files1 |
      "multiple with files" !! enrichments(enrichment1, nofiles, enrichment2) ! files1 ++ files2 |> {
      (_, enrichments, expectedFiles) =>
        {
          EnrichmentRegistry(enrichments).filesToCache.sorted must_== expectedFiles.sorted
        }

    }

  def enrichments(es: Enrichment*): EnrichmentMap =
    (es.indices.map(_.toString) zip es).toMap
}
