/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package storage.kinesis.elasticsearch

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// Snowplow
import enrich.common.utils.ScalazJson4sUtils

/**
 * Tests SnowplowElasticsearchTransformer
 */
class SnowplowElasticsearchTransformerSpec extends Specification with ValidationMatchers {

  val unstructJson = """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": {
      "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
      "data": {
        "targetUrl": "http://www.example.com",
        "elementClasses": ["foreground"],
        "elementId": "exampleLink"
      }
    }
  }"""

  val contextsJson = """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": [
      {
        "schema": "iglu:org.schema/WebPage/jsonschema/1-0-0",
        "data": {
          "genre": "blog",
          "inLanguage": "en-US",
          "datePublished": "2014-11-06T00:00:00Z",
          "author": "Fred Blundun",
          "breadcrumb": [
            "blog",
            "releases"
          ],
          "keywords": [
            "snowplow",
            "javascript",
            "tracker",
            "event"
          ]
        }
      },
      {
        "schema": "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0",
        "data": {
          "navigationStart": 1415358089861,
          "unloadEventStart": 1415358090270,
          "unloadEventEnd": 1415358090287,
          "redirectStart": 0,
          "redirectEnd": 0,
          "fetchStart": 1415358089870,
          "domainLookupStart": 1415358090102,
          "domainLookupEnd": 1415358090102,
          "connectStart": 1415358090103,
          "connectEnd": 1415358090183,
          "requestStart": 1415358090183,
          "responseStart": 1415358090265,
          "responseEnd": 1415358090265,
          "domLoading": 1415358090270,
          "domInteractive": 1415358090886,
          "domContentLoadedEventStart": 1415358090968,
          "domContentLoadedEventEnd": 1415358091309,
          "domComplete": 0,
          "loadEventStart": 0,
          "loadEventEnd": 0
        }
      }
    ]
  }"""

  val derivedContextsJson = """{
    "schema": "iglu:com.snowplowanalytics.snowplow\/contexts\/jsonschema\/1-0-1",
    "data": [
      {
        "schema": "iglu:com.snowplowanalytics.snowplow\/ua_parser_context\/jsonschema\/1-0-0",
        "data": {
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }
      }
    ]
  }"""

  "The 'fromClass' method" should {
    "successfully convert a tab-separated event string to JSON" in {
      val nvPairs = List(
      "app_id" -> "angry-birds",
      "platform" -> "web",
      "etl_tstamp" -> "2017-01-26 00:01:25.292",
      "collector_tstamp" -> "2013-11-26 00:02:05",
      "dvce_tstamp" -> "2013-11-26 00:03:57.885",
      "event" -> "page_view",
      "event_id" -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
      "txn_id" -> "41828",
      "name_tracker" -> "cloudfront-1",
      "v_tracker" -> "js-2.1.0",
      "v_collector" -> "clj-tomcat-0.1.0",
      "v_etl" -> "serde-0.5.2",
      "user_id" -> "jon.doe@email.com",
      "user_ipaddress" -> "92.231.54.234",
      "user_fingerprint" -> "2161814971",
      "domain_userid" -> "bc2e92ec6c204a14",
      "domain_sessionidx" -> "3",
      "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
      "geo_country" -> "US",
      "geo_region" -> "TX",
      "geo_city" -> "New York",
      "geo_zipcode" -> "94109",
      "geo_latitude" -> "37.443604",
      "geo_longitude" -> "-122.4124",
      "geo_region_name" -> "Florida",
      "ip_isp" -> "FDN Communications",
      "ip_organization" -> "Bouygues Telecom",
      "ip_domain" -> "nuvox.net",
      "ip_netspeed" -> "Cable/DSL",
      "page_url" -> "http://www.snowplowanalytics.com",
      "page_title" -> "On Analytics",
      "page_referrer" -> "",
      "page_urlscheme" -> "http",
      "page_urlhost" -> "www.snowplowanalytics.com",
      "page_urlport" -> "80",
      "page_urlpath" -> "/product/index.html",
      "page_urlquery" -> "id=GTM-DLRG",
      "page_urlfragment" -> "4-conclusion",
      "refr_urlscheme" -> "",
      "refr_urlhost" -> "",
      "refr_urlport" -> "",
      "refr_urlpath" -> "",
      "refr_urlquery" -> "",
      "refr_urlfragment" -> "",
      "refr_medium" -> "",
      "refr_source" -> "",
      "refr_term" -> "",
      "mkt_medium" -> "",
      "mkt_source" -> "",
      "mkt_term" -> "",
      "mkt_content" -> "",
      "mkt_campaign" -> "",
      "contexts" -> contextsJson,
      "se_category" -> "",
      "se_action" -> "",
      "se_label" -> "",
      "se_property" -> "",
      "se_value" -> "",
      "unstruct_event" -> unstructJson,
      "tr_orderid" -> "",
      "tr_affiliation" -> "",
      "tr_total" -> "",
      "tr_tax" -> "",
      "tr_shipping" -> "",
      "tr_city" -> "",
      "tr_state" -> "",
      "tr_country" -> "",
      "ti_orderid" -> "",
      "ti_sku" -> "",
      "ti_name" -> "",
      "ti_category" -> "",
      "ti_price" -> "",
      "ti_quantity" -> "",
      "pp_xoffset_min" -> "",
      "pp_xoffset_max" -> "",
      "pp_yoffset_min" -> "",
      "pp_yoffset_max" -> "",
      "useragent" -> "",
      "br_name" -> "",
      "br_family" -> "",
      "br_version" -> "",
      "br_type" -> "",
      "br_renderengine" -> "",
      "br_lang" -> "",
      "br_features_pdf" -> "1",
      "br_features_flash" -> "0",
      "br_features_java" -> "",
      "br_features_director" -> "",
      "br_features_quicktime" -> "",
      "br_features_realplayer" -> "",
      "br_features_windowsmedia" -> "",
      "br_features_gears" -> "",
      "br_features_silverlight" -> "",
      "br_cookies" -> "",
      "br_colordepth" -> "",
      "br_viewwidth" -> "",
      "br_viewheight" -> "",
      "os_name" -> "",
      "os_family" -> "",
      "os_manufacturer" -> "",
      "os_timezone" -> "",
      "dvce_type" -> "",
      "dvce_ismobile" -> "",
      "dvce_screenwidth" -> "",
      "dvce_screenheight" -> "",
      "doc_charset" -> "",
      "doc_width" -> "",
      "doc_height" -> "",
      "tr_currency" -> "",
      "tr_total_base" -> "",
      "tr_tax_base" -> "",
      "tr_shipping_base" -> "",
      "ti_currency" -> "",
      "ti_price_base" -> "",
      "base_currency" -> "",
      "geo_timezone" -> "",
      "mkt_clickid" -> "",
      "mkt_network" -> "",
      "etl_tags" -> "",
      "dvce_sent_tstamp" -> "",
      "refr_domain_userid" -> "",
      "refr_device_tstamp" -> "",
      "derived_contexts" -> derivedContextsJson,
      "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
      "derived_tstamp" -> "2013-11-26 00:03:57.886"
      )

      val eventValues = nvPairs.unzip._2.toArray

      val jsonRecord = new SnowplowElasticsearchTransformer("index", "type")
        .jsonifyGoodEvent(eventValues)
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val result = parse(jsonRecord.json)

      //throw new RuntimeException(pretty(render(result)))

      jsonRecord.id must beSome("c6ef3124-b53a-4b13-a233-0088f79dcbcb")

      val expected = parse("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_referrer" : null,
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "refr_urlscheme" : null,
        "refr_urlhost" : null,
        "refr_urlport" : null,
        "refr_urlpath" : null,
        "refr_urlquery" : null,
        "refr_urlfragment" : null,
        "refr_medium" : null,
        "refr_source" : null,
        "refr_term" : null,
        "mkt_medium" : null,
        "mkt_source" : null,
        "mkt_term" : null,
        "mkt_content" : null,
        "mkt_campaign" : null,
        "contexts_org_schema_web_page_1" : [ {
          "genre" : "blog",
          "inLanguage" : "en-US",
          "datePublished" : "2014-11-06T00:00:00Z",
          "author" : "Fred Blundun",
          "breadcrumb" : [ "blog", "releases" ],
          "keywords" : [ "snowplow", "javascript", "tracker", "event" ]
        } ],
        "contexts_org_w3_performance_timing_1" : [ {
          "navigationStart" : 1415358089861,
          "unloadEventStart" : 1415358090270,
          "unloadEventEnd" : 1415358090287,
          "redirectStart" : 0,
          "redirectEnd" : 0,
          "fetchStart" : 1415358089870,
          "domainLookupStart" : 1415358090102,
          "domainLookupEnd" : 1415358090102,
          "connectStart" : 1415358090103,
          "connectEnd" : 1415358090183,
          "requestStart" : 1415358090183,
          "responseStart" : 1415358090265,
          "responseEnd" : 1415358090265,
          "domLoading" : 1415358090270,
          "domInteractive" : 1415358090886,
          "domContentLoadedEventStart" : 1415358090968,
          "domContentLoadedEventEnd" : 1415358091309,
          "domComplete" : 0,
          "loadEventStart" : 0,
          "loadEventEnd" : 0
        } ],
        "se_category" : null,
        "se_action" : null,
        "se_label" : null,
        "se_property" : null,
        "se_value" : null,
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
          "targetUrl" : "http://www.example.com",
          "elementClasses" : [ "foreground" ],
          "elementId" : "exampleLink"
        },
        "tr_orderid" : null,
        "tr_affiliation" : null,
        "tr_total" : null,
        "tr_tax" : null,
        "tr_shipping" : null,
        "tr_city" : null,
        "tr_state" : null,
        "tr_country" : null,
        "ti_orderid" : null,
        "ti_sku" : null,
        "ti_name" : null,
        "ti_category" : null,
        "ti_price" : null,
        "ti_quantity" : null,
        "pp_xoffset_min" : null,
        "pp_xoffset_max" : null,
        "pp_yoffset_min" : null,
        "pp_yoffset_max" : null,
        "useragent" : null,
        "br_name" : null,
        "br_family" : null,
        "br_version" : null,
        "br_type" : null,
        "br_renderengine" : null,
        "br_lang" : null,
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "br_features_java" : null,
        "br_features_director" : null,
        "br_features_quicktime" : null,
        "br_features_realplayer" : null,
        "br_features_windowsmedia" : null,
        "br_features_gears" : null,
        "br_features_silverlight" : null,
        "br_cookies" : null,
        "br_colordepth" : null,
        "br_viewwidth" : null,
        "br_viewheight" : null,
        "os_name" : null,
        "os_family" : null,
        "os_manufacturer" : null,
        "os_timezone" : null,
        "dvce_type" : null,
        "dvce_ismobile" : null,
        "dvce_screenwidth" : null,
        "dvce_screenheight" : null,
        "doc_charset" : null,
        "doc_width" : null,
        "doc_height" : null,
        "tr_currency" : null,
        "tr_total_base" : null,
        "tr_tax_base" : null,
        "tr_shipping_base" : null,
        "ti_currency" : null,
        "ti_price_base" : null,
        "base_currency" : null,
        "geo_timezone" : null,
        "mkt_clickid" : null,
        "mkt_network" : null,
        "etl_tags" : null,
        "dvce_sent_tstamp" : null,
        "refr_domain_userid" : null,
        "refr_device_tstamp" : null,
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }],
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z"
      }""")

      // Specific fields
      ScalazJson4sUtils.extract[String](result, "platform") must beSuccessful("web")
      ScalazJson4sUtils.extract[Int](result, "domain_sessionidx") must beSuccessful(3)
      ScalazJson4sUtils.extract[Double](result, "geo_latitude") must beSuccessful(37.443604)
      ScalazJson4sUtils.extract[Boolean](result, "br_features_pdf") must beSuccessful(true)
      ScalazJson4sUtils.extract[Boolean](result, "br_features_flash") must beSuccessful(false)
      ScalazJson4sUtils.extract[String](result, "collector_tstamp") must beSuccessful("2013-11-26T00:02:05Z")
      ScalazJson4sUtils.extract[String](result, "geo_location") must beSuccessful("37.443604,-122.4124")
      ScalazJson4sUtils.extract[String](result, "ti_sku") must beSuccessful(null)

      // Unstructured event shredding
      result \ "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" \ "elementId" must_== JString("exampleLink")

      // Contexts shredding
      result \ "contexts_org_schema_web_page_1" \ "genre" must_== JString("blog")

      // The entire JSON
      result diff expected mustEqual Diff(JNothing, JNothing, JNothing)
    }
  }

}
