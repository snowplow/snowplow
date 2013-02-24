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

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Deserializer
import test.{SnowPlowDeserializer, SnowPlowEvent, SnowPlowTest}
import SnowPlowTest.DataGrid

class MarketingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Had to comment out first two test cases due to the parallel execution issue with Specs2
  val goodData: DataGrid = Map(/* "2012-08-06	20:35:07	IAD12	3343	82.26.22.236	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/11-oracles/genre-oracles/angels/type/all/view/grid?n=48&utm_source=GoogleSearch&utm_medium=cpc&utm_campaign=uk-oracle-decks--angel-cards-text&utm_term=buy%2520angel%2520cards&utm_content=27719551768&gclid=CPak27Ps07ECFQff4AodqX8AxA	Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:14.0)%20Gecko/20100101%20Firefox/14.0.1	page=Oracles%2520-%2520Angels%2520-%2520Psychic%2520Bazaar&tid=513983&duid=a1cb74ef2c1f3fb4&vid=1&lang=en-GB&refr=http%253A%252F%252Fwww.google.com%252Fuds%252Fafs%253Fq%253Dbuy%252520angel%252520cards%2526client%253Dizito_js%2526channel%253Dizito_uk_aw1c800%2526hl%253Den%2526adtest%253Doff%2526adsafe%253Doff%2526r%253Dm%2526adpage%253D1%2526adrep%253D2%2526gl%253Duk%2526oe%253DUTF-8%2526ie%253DUTF-8%2526fexp%253D21404%25252C53010%25252C38723%2526format%253Dp4%25257Cn4%2526ad%253Dn4a4%2526nocache%253D1344285142217%2526num%253D0%2526output%253Duds_ads_only%2526v%253D3%2526adlh%253Don%2526adext%253Das1%2526lines%253D3%2526rurl%253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%25252520angel%25252520cards%25252F%2526referer%253Dhttp%25253A%25252F%25252Fwww.google.co.uk%25252Faclk%25253Fsa%25253Dl%252526ai%25253DCV2zJAicgUK3ZGcij0AHdvoHACqOMhqwCw5vZ-jTunLwtEAYoCFCz6OG3BWC7vq6D0AqgAeXZzN8DyAEBqQIB6hFYNde0PqoEHk_Q0rOhQy3wAwpU--e0eDacJu5JDcHSrxK-bQ8SVg%252526num%25253D8%252526sig%25253DAOD64_17yH5Hs4VpRbQVl3AFQAWcfzYwWQ%252526ved%25253D0CKoBENEM%252526adurl%25253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%2525252520angel%2525252520cards%25252F%252526rct%25253Dj%252526q%25253Dangel%25252520cards%25252520where%25252520to%25252520buy%25252520in%25252520london%2526u_his%253D4%2526u_tz%253D60%2526dt%253D1344285142218%2526u_w%253D1680%2526u_h%253D1050%2526bs%253D1680%252C929%2526ps%253D1664%252C169%2526frm%253D0&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1680x1050&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utmsource%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dbuy%252520angel%252520cards%2526utm_content%253D27719551768%2526gclid%253DCPak27Ps07ECFQff4AodqX8AxA" ->
                                SnowPlowEvent("cpc", "GoogleSearch", "buy angel cards", "27719551768", "uk-oracle-decks--angel-cards-text"),
                              
                                "2012-08-06	20:35:26	IAD12	3343	82.26.22.236	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/11-oracles/genre-oracles/angels/type/all/view/grid?n=48&utm_source=GoogleSearch&utm_medium=cpc&utm_campaign=uk-oracle-decks--angel-cards-text&utm_term=buy%2520tarot%2520cards&utm_content=27719551768&gclid=CPak27Ps07ECFQff4AodqX8AxA	Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:14.0)%20Gecko/20100101%20Firefox/14.0.1	page=Oracles%2520-%2520Angels%2520-%2520Psychic%2520Bazaar&tid=749074&duid=a1cb74ef2c1f3fb4&vid=1&lang=en-GB&refr=http%253A%252F%252Fwww.google.com%252Fuds%252Fafs%253Fq%253Dbuy%252520angel%252520cards%2526client%253Dizito_js%2526channel%253Dizito_uk_aw1c800%2526hl%253Den%2526adtest%253Doff%2526adsafe%253Doff%2526r%253Dm%2526adpage%253D1%2526adrep%253D2%2526gl%253Duk%2526oe%253DUTF-8%2526ie%253DUTF-8%2526fexp%253D21404%25252C53010%25252C38723%2526format%253Dp4%25257Cn4%2526ad%253Dn4a4%2526nocache%253D1344285142217%2526num%253D0%2526output%253Duds_ads_only%2526v%253D3%2526adlh%253Don%2526adext%253Das1%2526lines%253D3%2526rurl%253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%25252520angel%25252520cards%25252F%2526referer%253Dhttp%25253A%25252F%25252Fwww.google.co.uk%25252Faclk%25253Fsa%25253Dl%252526ai%25253DCV2zJAicgUK3ZGcij0AHdvoHACqOMhqwCw5vZ-jTunLwtEAYoCFCz6OG3BWC7vq6D0AqgAeXZzN8DyAEBqQIB6hFYNde0PqoEHk_Q0rOhQy3wAwpU--e0eDacJu5JDcHSrxK-bQ8SVg%252526num%25253D8%252526sig%25253DAOD64_17yH5Hs4VpRbQVl3AFQAWcfzYwWQ%252526ved%25253D0CKoBENEM%252526adurl%25253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%2525252520angel%2525252520cards%25252F%252526rct%25253Dj%252526q%25253Dangel%25252520cards%25252520where%25252520to%25252520buy%25252520in%25252520london%2526u_his%253D4%2526u_tz%253D60%2526dt%253D1344285142218%2526u_w%253D1680%2526u_h%253D1050%2526bs%253D1680%252C929%2526ps%253D1664%252C169%2526frm%253D0&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1680x1050&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utmsource%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dbuy%252520angel%252520cards%2526utm_content%253D27719551768%2526gclid%253DCPak27Ps07ECFQff4AodqX8AxA" ->
                                SnowPlowEvent("cpc", "GoogleSearch", "buy tarot cards", "27719551768", "uk-oracle-decks--angel-cards-text"), */
                              
                                "2012-08-06	20:35:26	IAD12	3343	82.26.22.236	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/11-oracles/genre-oracles/angels/type/all/view/grid?n=48&utm_source=&utm_medium=cpc&utm_campaign=uk-oracle-decks--angel-cards-text&utm_term=buy%2520angel%2520cards&gclid=CPak27Ps07ECFQff4AodqX8AxA	Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:14.0)%20Gecko/20100101%20Firefox/14.0.1	page=Oracles%2520-%2520Angels%2520-%2520Psychic%2520Bazaar&tid=749074&duid=a1cb74ef2c1f3fb4&vid=1&lang=en-GB&refr=http%253A%252F%252Fwww.google.com%252Fuds%252Fafs%253Fq%253Dbuy%252520angel%252520cards%2526client%253Dizito_js%2526channel%253Dizito_uk_aw1c800%2526hl%253Den%2526adtest%253Doff%2526adsafe%253Doff%2526r%253Dm%2526adpage%253D1%2526adrep%253D2%2526gl%253Duk%2526oe%253DUTF-8%2526ie%253DUTF-8%2526fexp%253D21404%25252C53010%25252C38723%2526format%253Dp4%25257Cn4%2526ad%253Dn4a4%2526nocache%253D1344285142217%2526num%253D0%2526output%253Duds_ads_only%2526v%253D3%2526adlh%253Don%2526adext%253Das1%2526lines%253D3%2526rurl%253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%25252520angel%25252520cards%25252F%2526referer%253Dhttp%25253A%25252F%25252Fwww.google.co.uk%25252Faclk%25253Fsa%25253Dl%252526ai%25253DCV2zJAicgUK3ZGcij0AHdvoHACqOMhqwCw5vZ-jTunLwtEAYoCFCz6OG3BWC7vq6D0AqgAeXZzN8DyAEBqQIB6hFYNde0PqoEHk_Q0rOhQy3wAwpU--e0eDacJu5JDcHSrxK-bQ8SVg%252526num%25253D8%252526sig%25253DAOD64_17yH5Hs4VpRbQVl3AFQAWcfzYwWQ%252526ved%25253D0CKoBENEM%252526adurl%25253Dhttp%25253A%25252F%25252Fwww.izito.co.uk%25252Fwsuk%25252Faw1c800%25252Fbuy%2525252520angel%2525252520cards%25252F%252526rct%25253Dj%252526q%25253Dangel%25252520cards%25252520where%25252520to%25252520buy%25252520in%25252520london%2526u_his%253D4%2526u_tz%253D60%2526dt%253D1344285142218%2526u_w%253D1680%2526u_h%253D1050%2526bs%253D1680%252C929%2526ps%253D1664%252C169%2526frm%253D0&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1680x1050&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utmsource%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dbuy%252520angel%252520cards%2526gclid%253DCPak27Ps07ECFQff4AodqX8AxA" ->
                                new SnowPlowEvent().tap { e =>
                                  e.mkt_medium = "cpc"
                                  e.mkt_source = null
                                  e.mkt_term = "buy angel cards"
                                  e.mkt_content = null
                                  e.mkt_campaign = "uk-oracle-decks--angel-cards-text"
                                }
                              )

  "A SnowPlow event with marketing fields should have this marketing data extracted" >> {
    goodData foreach { case (row, expected) =>

      val actual = SnowPlowDeserializer.deserialize(row)

      "The SnowPlow row \"%s\"".format(row) should {
        "have mkt_medium = %s".format(expected.mkt_medium) in {
          actual.mkt_medium must_== expected.mkt_medium
        }
        "have mkt_campaign = %s".format(expected.mkt_campaign) in {
          actual.mkt_campaign must_== expected.mkt_campaign
        }
        "have mkt_source = %s".format(expected.mkt_source) in {
          actual.mkt_source must_== expected.mkt_source
        }
        "have mkt_term = %s".format(expected.mkt_term) in {
          actual.mkt_term must_== expected.mkt_term
        }
        "have mkt_content = %s".format(expected.mkt_content) in {
          actual.mkt_content must_== expected.mkt_content
        }
      }
    }
  }
}

