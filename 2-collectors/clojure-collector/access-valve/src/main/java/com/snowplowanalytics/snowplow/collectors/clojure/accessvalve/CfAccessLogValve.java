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
package com.snowplowanalytics.snowplow.collectors.clojure.accessvalve;

// Java
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

// Tomcat
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

// Get our project settings
import com.snowplowanalytics.snowplow.collectors.clojure.accessvalve.generated.Version;

/**
 * A custom AccessLogValve for Tomcat to help generate CloudFront-like access logs.
 * Used in SnowPlow's Clojure Collector.
 *
 * Introduces a new pattern, 'I', to escape an incoming header
 * Introduces a new pattern, 'C', to fetch a cookie stored on the response
 * Re-implements the pattern 'i' to ensure that "" (empty string) is replaced with "-"
 * Re-implements the pattern 'q' to remove the "?" and ensure "" (empty string) is replaced with "-"
 * Overwrites the 'v' pattern, to write the version of this AccessLogValve, rather than the local server name
 *
 * This is adapted from the original AccessLogValve code here:
 * http://javasourcecode.org/html/open-source/tomcat/tomcat-7.0.29/org/apache/catalina/valves/AccessLogValve.java.html
 */
public class CfAccessLogValve extends AccessLogValve {

    private static final String cfEncoding = "UTF-8";

    /**
     * Create an AccessLogElement implementation which needs header string.
     *
     * Changes:
     * - Added 'I' pattern, to escape an incoming header
     * - Added 'C' pattern, to fetch a cookie on the response (not request)
     * - Fixed 'i' pattern, to replace "" (empty string) with "-"
     */
    @Override
    protected AccessLogElement createAccessLogElement(String header, char pattern) {

        switch (pattern) {
            // A safer header element returns a hyphen never a null/empty string
            case 'i':
                return new SaferHeaderElement(header);
            // Added EscapedHeaderElement        
            case 'I':
                return new EscapedHeaderElement(header);
            // Added EscapedHeaderElement        
            case 'C':
                return new ResponseCookieElement(header);
            // Back to AccessLogValve's handler
            default:
                return super.createAccessLogElement(header, pattern);
        }
    }

    /**
     * create an AccessLogElement implementation which doesn't need a header string.
     *
     * Changes:
     * - Fixed 'q' pattern, to remove the "?" and ensure "" (empty string) is replaced with "-"
     * - Overwrote 'v' pattern, to write the version of this AccessLogValve, rather than the local server name  
     */
    @Override
    protected AccessLogElement createAccessLogElement(char pattern) {
        switch (pattern) {
            // A better (safer, Cf-compatible) querystring element
            case 'q':
                return new BetterQueryElement();
            // Return the version of this AccessLogValve
            case 'v':
                return new ValveVersionElement();
            // Back to AccessLogValve's handler
            default:
                return super.createAccessLogElement(pattern);
        }
    }

    /**
     * We replace writing the local server name with writing
     * the version of this Tomcat AccessLogValve - %v
     */
    protected static class ValveVersionElement implements AccessLogElement {
        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {
            buf.append("tom-");
            buf.append(Version.VERSION);
        }
    }

    /**
     * Write incoming headers - %{xxx}i
     * Makes sure to hyphenate in the case of null/empty element.
     *
     * Note: if there are multiple blank elements, then -,-,- or
     * similar will be returned. This is acceptable for our
     * (Snowplow's) purposes - your use case might differ.
     */
    protected static class SaferHeaderElement implements AccessLogElement {
        private final String header;

        public SaferHeaderElement(String header) {
            this.header = header;
        }

        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {
            Enumeration<String> iter = request.getHeaders(header);
            if (iter.hasMoreElements()) {
                buf.append(handleBlankSafely(iter.nextElement()));
                while (iter.hasMoreElements()) {
                    buf.append(',').append(handleBlankSafely(iter.nextElement()));
                }
                return;
            }
            buf.append('-');
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
     * Write Querystring _without_ an initial '?', for compatibility with CloudFront - %q
     * Makes sure to hyphenate in the case of null/empty element.
     */
    protected static class BetterQueryElement implements AccessLogElement {
        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {
            if (request != null) {
                String query = request.getQueryString();
                if (query != null) {
                    buf.append(query);
                    return;
                }
            }
            buf.append('-');
        }
    }

    /**
     * Write a cookie on the response - %{xxx}C.
     *
     * Assumes cookies are pre-escaped.
     *
     * This is hacky code - literally the most primitive
     * cookie parsing possible. Please test it with the
     * cookies you want to log to check it works for you.
     */
    protected static class ResponseCookieElement implements AccessLogElement {
        private final String header;

        public ResponseCookieElement(String header) {
            this.header = header;
        }

        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {
            Iterator<String> iter = response.getHeaders("Set-Cookie").iterator();
            while (iter.hasNext()) {
                final String cookie = iter.next();
                // Yech. You should test this works with the cookies you want to log.
                if (cookie.startsWith(header + "=")) {
                    buf.append(cookie.split(";")[0].split("=")[1]);
                    return; // Multiple cookies with the same name? We don't support that.
                }
            }
            buf.append('-');
        }
    }

    /**
     * Replaces a null or empty string with
     * a hyphen, to avoid the logs ending up
     * with any <TAB><TAB> entries (which can
     * break parsers).
     *
     * @param s The String to hyphenate if blank
     * @return The string, or a hyphen if blank
     */
    protected static String handleBlankSafely(String s) {
        if (s == null || s.isEmpty()) {
            return "-";
        } else {
            return s;
        }
    }

    /**
     * Encodes a string or returns a "-" if not possible.
     *
     * @param s The String to encode
     * @return The encoded string
     */
    protected static String encodeStringSafely(String s) {
        try {
            return URLEncoder.encode(s, cfEncoding);
        } catch (Exception e) {
            return "-";
        }
    }    
}