package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package registry

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables

/**
 * Tests the convertBrowserLanguage function
 */
class BrowserLanguageEnrichmentSpec extends Specification with DataTables {

  def is =
    "Converting browser language across a variety of languages should work"     ! e1

  def e1 =
    "SPEC NAME"              || "BROWSER LANG"          | "EXPECTED OUTPUT"     |
    "none"                   !! ""                      ! ""                    |
    "english"                !! "en"                    ! "English"             |
    "english US"             !! "en-US"                 ! "English"             |
    "english GB"             !! "en-GB"                 ! "English"             |
    "english ZA"             !! "en-ZA"                 ! "English"             |
    "english us"             !! "en-us"                 ! "English"             |
    "french"                 !! "fr"                    ! "French"              |
    "french FR"              !! "fr-FR"                 ! "French"              |
    "russian"                !! "ru"                    ! "Russian"             |
    "chinese cn"             !! "zh-cn"                 ! "Chinese"             |
    "german"                 !! "de"                    ! "German"              |> {
      (_, browser_language, expected) => {
        BrowserLanguageEnrichment.convertBrowserLanguage(browser_language) must_== expected
      }
    }
}
