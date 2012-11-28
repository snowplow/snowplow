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

// TODO: delete these
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.MatchResult;

import java.net.URLEncoder;

import java.util.Date;
import java.util.Enumeration;

import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

// https://github.com/snowplow/snowplow/blob/master/3-etl/hive-etl/snowplow-log-deserializers/src/main/java/com/snowplowanalytics/snowplow/hadoop/hive/SnowPlowEventStruct.java

public class CfAccessValve extends AccessLogValve {

    private static Pattern pattern = Pattern.compile("\\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})\\b");

    /*
     This method will sanitize any cc numbers in the string and replace them with x's
    */
    private String sanitize(String string) {
        String sanitizedString = string;

        if(string != null) {

            StringBuffer buffer = new StringBuffer();
            Matcher matcher = pattern.matcher(string);

            while(matcher.find()) {
                MatchResult matchResult = matcher.toMatchResult();

                int start = matchResult.start();
                int end = matchResult.end();

                String matchedText = string.substring(start, end);

                matcher.appendReplacement(buffer, "xxxxxxxxxxxxxxxx");                
            }

            matcher.appendTail(buffer);

            sanitizedString = buffer.toString();
        }

        return sanitizedString;
    }

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

    @Override
    public void log(String message) {
        super.log(sanitize(message));
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