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

class BadMarketingTest extends Specification {

	// Toggle if tests are failing and you want to inspect the struct contents
	implicit val _DEBUG = false

	val badRow = "2012-09-06 04:08:47  LHR5  3345  188.221.220.27  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/2-tarot-cards/genre/contemporary/type/all/view/grid?n=48?utmsource=GoogleSearch&utm_medium=cpc&utm_campaign=uk-tarot--new-tarot-decks&utm_term=new%2520tarot%2520decks&utm_content=28648839928&gclid=CJfR5KyKoLICFSfMtAodIRQAJg  Mozilla/5.0%20(Windows%20NT%206.0;%20WOW64;%20rv:14.0)%20Gecko/20100101%20Firefox/14.0.1  page=Tarot%2520cards%2520-%2520Contemporary%2520-%2520Psychic%2520Bazaar&tid=843970&duid=6dc9f51b0ed1749a&vid=1&lang=en-GB&refr=http%253A%252F%252Fwww.google.co.uk%252Faclk%253Fsa%253Dl%2526ai%253DCcIU-tyFIUL-qNMW9hAeF7YHYDLjn594DkPP28Gq92Pi7CRAFILZUKAZQrNHb1gJgu861g9AKoAHo7JvZA8gBAakC0eZ4iZGsuj6qBCFP0GDWftmeAO1lyTL57ZoywAUjfg7BkEf6jTLgtPiYQBWABZBO%2526num%253D7%2526sig%253DAOD64_0l3q7DdSrX63qHR-b1zWgHA6ACGg%2526ved%253D0CMwBENEM%2526adurl%253Dhttp%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fcontemporary%252Ftype%252Fall%252Fview%252Fgrid%25253Fn%25253D48%25253Futmsource%25253DGoogleSearch%252526utm_medium%25253Dcpc%252526utm_campaign%25253Duk-tarot--new-tarot-decks%252526utm_term%25253Dnew%25252520tarot%25252520decks%252526utm_content%25253D28648839928%2526rct%253Dj%2526q%253Dtarot%252Bcards&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x1024&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fcontemporary%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%253Futmsource%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-tarot--new-tarot-decks%2526utm_term%253Dnew%252520tarot%252520decks%2526utm_content%253D28648839928%2526gclid%253DCJfR5KyKoLICFSfMtAodIRQAJg"

	"A SnowPlow event where the calling page's querystring is malformed" should {

		val actual = SnowPlowDeserializer.deserialize(badRow)

		"return <<null>> marketing attribution fields" in {
			actual.mkt_medium must beNull
			actual.mkt_campaign must beNull
			actual.mkt_source must beNull
			actual.mkt_term must beNull
			actual.mkt_content must beNull
		}
	}
}
