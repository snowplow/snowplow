/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive

// Specs2
import org.specs2.mutable.Specification

// Deserializer
import test.SnowPlowDeserializer

class CorruptUrlTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val linewithpercent = "2012-09-01 04:46:27  LAX3  3329  64.124.98.10  GET d1gk54bwlds2s3.cloudfront.net /ice.png?ev_ca=Homepage%2520Video&ev_ac=undefined&ev_la=%252Fvideo-homepage&ev_pr=0&ev_va=undefined&tid=425848&duid=287e370311a34439&vid=1&lang=en-US&refr=http%253A%252F%252Fwww.google.com%252Faclk%253Fsa%253Dl%2526ai%253DCrbk5g5BBUMeZHoeyiwKC0YGYCaHggswCweH5kzKv0-4FCAAQASgDUKrGgbQGYMmO-IbIo_waoAGfo-neA8gBAaoEIE_Qbs4IKEnipO7Cvt2JTuxoVa7lTe4METP5C_IZOvDAugUTCPr85_rGk7ICFYIUQgodJh0A4soFAA%2526rct%253Dj%2526q%253Dlogo%252520design%2526ei%253Dg5BBULqvHIKpiAKmuoCQDg%2526sig%253DAOD64_2VoU98tDbLVl7dhnDEKhJ4RjzOPA%2526sqi%253D2%2526ved%253D0CB4Q0Qw%2526adurl%253Dhttp%253A%252F%252Fmydomain.com%252F%25253Foptimizely%252526utm_medium%25253Dcpc%252526utm_source%25253Dadwords%25252Bsearch%252526utm_campaign%25253DLogo%25252BDesign%25252B-%25252BUS%252526utm_content%25253DLogo%25252BDesign%25252BExact%252526utm_creative%25253D13380598353%252526utm_target%25253D%252526utm_term%25253Dlogo%25252520design%252526utm_placement%25253D%2526cad%253Drja&f_pdf=0&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cookie=1&url=http%253A%252F%252Fmydomain.com%252F%253Foptimizely%2526utm_medium%253Dcpc%2526utm_source%253Dadwords%25 200 - Mozilla/5.0%20(Windows;%20U;%20Windows%20NT%205.1;%20en-US;%20rv:1.9.0.1)%20Gecko/2008070208%20Firefox/3.0.1  ev_ca=Homepage%2520Video&ev_ac=undefined&ev_la=%252Fvideo-homepage&ev_pr=0&ev_va=undefined&tid=425848&duid=287e370311a34439&vid=1&lang=en-US&refr=http%253A%252F%252Fwww.google.com%252Faclk%253Fsa%253Dl%2526ai%253DCrbk5g5BBUMeZHoeyiwKC0YGYCaHggswCweH5kzKv0-4FCAAQASgDUKrGgbQGYMmO-IbIo_waoAGfo-neA8gBAaoEIE_Qbs4IKEnipO7Cvt2JTuxoVa7lTe4METP5C_IZOvDAugUTCPr85_rGk7ICFYIUQgodJh0A4soFAA%2526rct%253Dj%2526q%253Dlogo%252520design%2526ei%253Dg5BBULqvHIKpiAKmuoCQDg%2526sig%253DAOD64_2VoU98tDbLVl7dhnDEKhJ4RjzOPA%2526sqi%253D2%2526ved%253D0CB4Q0Qw%2526adurl%253Dhttp%253A%252F%252Fmydomain.com%252F%25253Foptimizely%252526utm_medium%25253Dcpc%252526utm_source%25253Dadwords%25252Bsearch%252526utm_campaign%25253DLogo%25252BDesign%25252B-%25252BUS%252526utm_content%25253DLogo%25252BDesign%25252BExact%252526utm_creative%25253D13380598353%252526utm_target%25253D%252526utm_term%25253Dlogo%25252520design%252526utm_placement%25253D%2526cad%253Drja&f_pdf=0&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cookie=1&url=http%253A%252F%252Fmydomain.com%252F%253Foptimizely%2526utm_medium%253Dcpc%2526utm_source%253Dadwords%25";

  "A URL with a % at the end" should {
    "not be null" in {
        SnowPlowDeserializer.deserialize(linewithpercent).asInstanceOf[SnowPlowEventStruct].page_url must not beNull
    }
  }
}
