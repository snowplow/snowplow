/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark
package bad

import org.specs2.mutable.Specification

object NullNumericFieldsSpec {
  val lines = EnrichJobSpec.Lines(
    "2014-10-11  14:01:05    -   37  172.31.38.31    GET 24.209.95.109   /i  200 http://www.myvideowebsite.com/embed/ab123456789?auto_start=e9&rf=cb Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10.6%3B+rv%3A32.0%29+Gecko%2F20100101+Firefox%2F32.0   e=se&se_ca=video-player%3Anewformat&se_ac=play-time&se_la=efba3ef384&se_va=&tid="
  )
  val expected =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/enrichment_failures/jsonschema/1-0-0","data":{"processor":{"artifact":"spark","version":"${generated.BuildInfo.version}"},"failure":{"timestamp":"2019-11-22T09:37:21.643Z","messages":[{"enrichment":null,"message":{"field":"tid","value":"","expectation":"not a valid integer"}},{"enrichment":null,"message":{"field":"se_va","value":"","expectation":"cannot be converted to Double-like"}}]},"payload":{"enriched":{"app_id":null,"platform":null,"etl_tstamp":"2001-09-09 01:46:40.000","collector_tstamp":"2014-10-11 14:01:05.000","dvce_created_tstamp":null,"event":"struct","event_id":"ff30c659-682a-4bf6-a378-4081d3c34c76","txn_id":null,"name_tracker":null,"v_tracker":null,"v_collector":"clj-tomcat","v_etl":"spark-${generated.BuildInfo.version}-common-1.0.0","user_id":null,"user_ipaddress":"172.31.38.31","user_fingerprint":null,"domain_userid":null,"domain_sessionidx":0,"network_userid":null,"geo_country":null,"geo_region":null,"geo_city":null,"geo_zipcode":null,"geo_latitude":0.0,"geo_longitude":0.0,"geo_region_name":null,"ip_isp":null,"ip_organization":null,"ip_domain":null,"ip_netspeed":null,"page_url":"http://www.myvideowebsite.com/embed/ab123456789?auto_start=e9&rf=cb","page_title":null,"page_referrer":null,"page_urlscheme":"http","page_urlhost":"www.myvideowebsite.com","page_urlport":80,"page_urlpath":"/embed/ab123456789","page_urlquery":"auto_start=e9&rf=cb","page_urlfragment":null,"refr_urlscheme":null,"refr_urlhost":null,"refr_urlport":0,"refr_urlpath":null,"refr_urlquery":null,"refr_urlfragment":null,"refr_medium":null,"refr_source":null,"refr_term":null,"mkt_medium":null,"mkt_source":null,"mkt_term":null,"mkt_content":null,"mkt_campaign":null,"contexts":null,"se_category":"video-player:newformat","se_action":"play-time","se_label":"efba3ef384","se_property":null,"se_value":null,"unstruct_event":null,"tr_orderid":null,"tr_affiliation":null,"tr_total":null,"tr_tax":null,"tr_shipping":null,"tr_city":null,"tr_state":null,"tr_country":null,"ti_orderid":null,"ti_sku":null,"ti_name":null,"ti_category":null,"ti_price":null,"ti_quantity":0,"pp_xoffset_min":0,"pp_xoffset_max":0,"pp_yoffset_min":0,"pp_yoffset_max":0,"useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:32.0) Gecko/20100101 Firefox/32.0","br_name":"Firefox 32","br_family":"Firefox","br_version":"32.0","br_type":"Browser","br_renderengine":"GECKO","br_lang":null,"br_features_pdf":0,"br_features_flash":0,"br_features_java":0,"br_features_director":0,"br_features_quicktime":0,"br_features_realplayer":0,"br_features_windowsmedia":0,"br_features_gears":0,"br_features_silverlight":0,"br_cookies":0,"br_colordepth":null,"br_viewwidth":0,"br_viewheight":0,"os_name":"Mac OS X","os_family":"Mac OS X","os_manufacturer":"Apple Inc.","os_timezone":null,"dvce_type":"Computer","dvce_ismobile":0,"dvce_screenwidth":0,"dvce_screenheight":0,"doc_charset":null,"doc_width":0,"doc_height":0,"tr_currency":null,"tr_total_base":null,"tr_tax_base":null,"tr_shipping_base":null,"ti_currency":null,"ti_price_base":null,"base_currency":null,"geo_timezone":null,"mkt_clickid":null,"mkt_network":null,"etl_tags":null,"dvce_sent_tstamp":null,"refr_domain_userid":null,"refr_dvce_tstamp":null,"derived_contexts":"{\\"schema\\":\\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1\\",\\"data\\":[{\\"schema\\":\\"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0\\",\\"data\\":{\\"useragentFamily\\":\\"Firefox\\",\\"useragentMajor\\":\\"32\\",\\"useragentMinor\\":\\"0\\",\\"useragentPatch\\":null,\\"useragentVersion\\":\\"Firefox 32.0\\",\\"osFamily\\":\\"Mac OS X\\",\\"osMajor\\":\\"10\\",\\"osMinor\\":\\"6\\",\\"osPatch\\":null,\\"osPatchMinor\\":null,\\"osVersion\\":\\"Mac OS X 10.6\\",\\"deviceFamily\\":\\"Other\\"}}]}","domain_sessionid":null,"derived_tstamp":"2014-10-11 14:01:05.000","event_vendor":"com.google.analytics","event_name":"event","event_format":"jsonschema","event_version":"1-0-0","event_fingerprint":"15e7254294d59796845177fe556eaeca","true_tstamp":null},"raw":{"vendor":"com.snowplowanalytics.snowplow","version":"tp1","parameters":{"e":"se","se_va":"","se_ac":"play-time","se_la":"efba3ef384","se_ca":"video-player:newformat","tid":""},"contentType":null,"loaderName":"clj-tomcat","encoding":"UTF-8","hostname":null,"timestamp":"2014-10-11T14:01:05.000Z","ipAddress":"172.31.38.31","useragent":"Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10.6%3B+rv%3A32.0%29+Gecko%2F20100101+Firefox%2F32.0","refererUri":"http://www.myvideowebsite.com/embed/ab123456789?auto_start=e9&rf=cb","headers":[],"userId":null}}}}"""
}

/**
 * Check that all tuples in a custom structured event (CloudFront format) are successfully
 * extracted.
 */
class NullNumericFieldsSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "null-numeric-fields"
  sequential
  "A job which processes a CF file containing 1 event with null int and double fields" should {
    runEnrichJob(NullNumericFieldsSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "write a bad row JSON containing the input line and all errors" in {
      val Some(bads) = readPartFile(dirs.badRows)
      removeId(removeTstamp(bads.head)) must_== NullNumericFieldsSpec.expected
    }

    "not write any events" in {
      dirs.output must beEmptyDir
    }
  }
}
