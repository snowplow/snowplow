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

package com.snowplowanalytics.tomcat;

import java.net.URLEncoder;
import java.util.Date;
import java.util.Enumeration;

import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

// https://github.com/snowplow/snowplow/blob/master/3-etl/hive-etl/snowplow-log-deserializers/src/main/java/com/snowplowanalytics/snowplow/hadoop/hive/SnowPlowEventStruct.java

public class CfAccessValve extends AccessLogValve {

/**
        * create an AccessLogElement implementation which needs header string
   http://svn.apache.org/repos/asf/tomcat/tc6.0.x/tags/TOMCAT_6_0_33/java/org/apache/catalina/valves/AccessLogValve.java
        */
    protected AccessLogElement createAccessLogElement(String header, char pattern) {
        switch (pattern) {
        case 'i':
            return new HeaderElement(header);
        case 'e':
            return new HeaderElement(header);
        case 'c':
            return new CookieElement(header);
        case 'o':
            return new ResponseHeaderElement(header);
        case 'r':
            return new RequestAttributeElement(header);
        case 's':
            return new SessionAttributeElement(header);            
        default:
            return new StringElement("???");
        }
    }

    /**
     * write incoming headers - %{xxx}i
     */
    protected class HeaderElement implements AccessLogElement {
        private String header;

        public HeaderElement(String header) {
            this.header = header;
        }

        public void addElement(StringBuffer buf, Date date, Request request,
                Response response, long time) {
            Enumeration<String> iter = request.getHeaders(header);
            if (iter.hasMoreElements()) {
                buf.append(iter.nextElement());
                while (iter.hasMoreElements()) {
                    buf.append(',').append(iter.nextElement());
                }
                return;
            }
            buf.append('-');
        }
    }
}