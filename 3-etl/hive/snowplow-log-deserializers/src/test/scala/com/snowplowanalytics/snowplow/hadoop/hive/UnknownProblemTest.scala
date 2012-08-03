/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

class UnknownProblemTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // Not sure what is wrong with this row, but it's erroring
  val badRows = Set(
    "2012-06-09	00:12:34	EWR2	3402	184.151.127.250	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/tarot-cards/127-petrak-tarot.html	Mozilla/5.0%20(Macintosh;%20U;%20Intel%20Mac%20OS%20X%2010_6_2;%20en-us)%20AppleWebKit/531.21.8%20(KHTML,%20like%20Gecko)%20Version/4.0.4%20Safari/531.21.10	page=%250A%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520Petrak%2520Tarot%2520-%2520Psychic%2520Bazaar%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520&tid=248389&uid=825c94ab288ad859&vid=5&lang=en-us&refr=http%253A%252F%252Fwww.google.ca%252Fimgres%253Fimgurl%253Dhttp%253A%252F%252Fmdm.pbzstatic.com%252Ftarot%252Fpetrak-tarot%252Fcard-4.png%2526imgrefurl%253Dhttp%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F127-petrak-tarot.html%2526usg%253D__ShZoNSQ7LBtLdOusrFw2LelwXiI%253D%2526h%253D1551%2526w%253D1064%2526sz%253D1526%2526hl%253Den%2526start%253D14%2526zoom%253D1%2526tbnid%253DcxpuKsYgiS1TPM%253A%2526tbnh%253D150%2526tbnw%253D103%2526ei%253D2JTST-bYKMa-0QHTwZyEAw%2526prev%253D%252Fsearch%25253Fq%25253Dpetra%25252BK%25252Bpiatnik%25252Btarot%252526hl%25253Den%252526client%25253Dsafari%252526sa%25253DX%252526rls%25253Den%252526tbm%25253Disch%252526prmd%25253Divns%2526itbs%253D1&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F127-petrak-tarot.html",
    "2012-06-19	01:34:44	IND6	3402	184.151.114.145	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=7	Mozilla/5.0%20(Macintosh;%20U;%20Intel%20Mac%20OS%20X%2010_6_2;%20en-us)%20AppleWebKit/531.21.8%20(KHTML,%20like%20Gecko)%20Version/4.0.4%20Safari/531.21.10	page=%250A%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520Tarot%2520cards%2520-%2520Psychic%2520Bazaar%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520&tid=870480&uid=825c94ab288ad859&vid=6&lang=en-us&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fp%253D6&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fp%253D7",
    "2012-06-25	08:39:38	LHR3	3345	86.182.192.39	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/search?search_query=rider+waite&x=11&y=18&p=2	Mozilla/5.0%20(Macintosh;%20U;%20PPC%20Mac%20OS%20X%2010_5_5;%20en-us)%20AppleWebKit/525.18%20(KHTML,%20like%20Gecko)%20Version/3.1.2%20Safari/525.20.1	page=%250A%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520Search%2520-%2520Psychic%2520Bazaar%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520&tid=038467&uid=8be3869233ad515e&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.psychicbazaar.com%252Fsearch%253Fsearch_query%253Drider%252Bwaite%2526x%253D11%2526y%253D18&f_pdf=0&f_qt=1&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Fsearch%253Fsearch_query%253Drider%252Bwaite%2526x%253D11%2526y%253D18%2526p%253D2",
    "2012-07-04	03:45:58	JFK5	3402	184.151.127.253	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/	Mozilla/5.0%20(Macintosh;%20U;%20Intel%20Mac%20OS%20X%2010_6_2;%20en-us)%20AppleWebKit/531.21.8%20(KHTML,%20like%20Gecko)%20Version/4.0.4%20Safari/531.21.10",
    "2012-06-25	08:37:33	LHR3	3345	86.182.192.39	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/search?search_query=mini+rider+waite&x=10&y=18	Mozilla/5.0%20(Macintosh;%20U;%20PPC%20Mac%20OS%20X%2010_5_5;%20en-us)%20AppleWebKit/525.18%20(KHTML,%20like%20Gecko)%20Version/3.1.2%20Safari/525.20.1	page=%250A%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520Search%2520-%2520Psychic%2520Bazaar%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520%2520&tid=953361&uid=8be3869233ad515e&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F57-universal-rider-waite-pocket-tarot-deck.html&f_pdf=0&f_qt=1&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Fsearch%253Fsearch_query%253Dmini%252Brider%252Bwaite%2526x%253D10%2526y%253D18"
  )
    
  "Our SnowPlow rows which are erroring" >> {
    badRows.foreach { row => 
      "should not return a <<null>> record" in {
        SnowPlowEventDeserializer.deserializeLine(row, DEBUG).asInstanceOf[SnowPlowEventStruct].dt must beNull
      }
    }
  }
}


