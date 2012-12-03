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

// Java
import java.util.Date;
import java.util.Enumeration;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

// Tomcat
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

/**
 * A custom AccessLogValve for Tomcat to help generate CloudFront-like access logs.
 *
 * Introduces a new pattern, 'I', to escape an incoming header.
 * Used in SnowPlow's Clojure Collector to escape the User-Agent header (as 
 * CloudFront does).
 *
 * All original AccessLogValve code taken from:
 * http://javasourcecode.org/html/open-source/tomcat/tomcat-7.0.29/org/apache/catalina/valves/AccessLogValve.java.html
 */
public class CfAccessLogValve extends AccessLogValve {

    private static final String cfEncoding = "UTF-8";

    /**
     * Create an AccessLogElement implementation which needs header string.
     * Updated to include an 'I' pattern, to escape an incoming header.
     */
    protected AccessLogElement createAccessLogElement(String header, char pattern) {

        // Added EscapedHeaderElement        
        if (pattern == 'I') {
            return new EscapedHeaderElement(header);
        } else {
            return super.createAccessLogElement(header, pattern);
        }
    }

    /**
     * Write incoming headers, but escaped - %{xxx}I
     * Based on HeaderElement.
     */
    protected static class EscapedHeaderElement implements AccessLogElement {
        private final String header;

        public EscapedHeaderElement(String header) {
            this.header = header;
        }

        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {
            Enumeration<String> iter = request.getHeaders(header);
            if (iter.hasMoreElements()) {
                buf.append(encodeStringSafely(iter.nextElement()));
                while (iter.hasMoreElements()) {
                    buf.append(',').append(encodeStringSafely(iter.nextElement()));
                }
                return;
            }
            buf.append('-');
        }
    }

    /**
    * Encodes a string or returns a "-" if not possible.
    *
    * @param s The String to encode
    * @return The encoded string
    */
    protected static String encodeStringSafely(String s)
    {
        try {
            return URLEncoder.encode(s, cfEncoding);
        } catch (Exception e) {
            return "-";
        }
    }    
}