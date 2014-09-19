/* 
 * Copyright (c) 2012-2014 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.clojure;

// Java
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.net.URLEncoder;

// Apache Commons
import org.apache.commons.codec.binary.Base64;

// Tomcat, Catalina and Coyote
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.tomcat.util.buf.ByteChunk;

// This project
import com.snowplowanalytics.snowplow.collectors.clojure.generated.ProjectSettings;

/**
 * A custom AccessLogValve for Tomcat to help generate CloudFront-like access logs.
 * Used in SnowPlow's Clojure Collector.
 *
 * Introduces a new pattern, 'I', to escape an incoming header
 * Introduces a new pattern, 'C', to fetch a cookie stored on the response
 * Introduces a new pattern, 'w' to capture the request's body
 * Introduces a new pattern, '~' to capture the request's content type
 * Re-implements the pattern 'i' to ensure that "" (empty string) is replaced with "-"
 * Re-implements the pattern 'q' to remove the "?" and ensure "" (empty string) is replaced with "-"
 * Overwrites the 'v' pattern, to write the version of this AccessLogValve, rather than the local server name
 *
 * This is adapted from the original AccessLogValve code here:
 * http://grepcode.com/file/repo1.maven.org/maven2/org.apache.tomcat/tomcat-catalina/7.0.29/org/apache/catalina/valves/AccessLogValve.java
 */
public class SnowplowAccessLogValve extends AccessLogValve {

    /**
     * Create an AccessLogElement implementation which operates on a
     * header string.
     *
     * Changes:
     * - Added 'I' pattern, to escape an incoming header
     * - Added 'C' pattern, to fetch a cookie on the response (not request)
     * - Fixed 'i' pattern, to replace "" (empty string) with "-"
     * - Added 'w' pattern, to capture the request's body
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
     * Create an AccessLogElement implementation which doesn't need a header string.
     *
     * Changes:
     * - Fixed 'q' pattern, to remove the "?" and ensure "" (empty string) is replaced with "-"
     * - Overwrote 'v' pattern, to write the version of this AccessLogValve, rather than the local server name
     * - Added 'w' pattern, to return the request's body
     * - Added '~' pattern, to capture the request's content type
     * - Overwrote 'a' pattern, to get remote IP more reliably, even through proxies
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
            // Return the request body
            case 'w':
                return new Base64EncodedBodyElement();
            // Return the content type of the request
            case '~':
                return new EscapedContentTypeElement();
            // Return the remote IP address
            case 'a':
                return new ProxyAwareRemoteAddrElement();
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
            buf.append(ProjectSettings.VERSION);
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
                buf.append(uriEncodeSafely(iter.nextElement()));
                while (iter.hasMoreElements()) {
                    buf.append(',').append(uriEncodeSafely(iter.nextElement()));
                }
                return;
            }
            buf.append('-');
        }
    }

    /**
     * Write Querystring _without_ an initial '?', for compatibility with CloudFront - %q
     * Makes sure to leave empty ("") in case of null/empty element - necessary because
     * we will be manually appending some values to the querystring in the server.xml,
     * and we don't want "-&amp;cv=clj-0.7.0-tom-0.1.0&amp;..."
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
            buf.append(""); // No hyphen
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
     * A new element - returns the content-type
     * for the request's body, Base64-URL-safe-encoded.
     */
    protected static class EscapedContentTypeElement implements AccessLogElement {
        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {

            final String contentType = request.getContentType();
            if (contentType != null) {
                buf.append(uriEncodeSafely(contentType));
                return;
            }
            buf.append('-');
        }
    }

    /**
     * Write remote IP address - %a.
     * Will check first for an X-Forwarded-For header, and use that
     * if set. If not set, will revert to using the standard remote
     * IP address.
     */
    protected class ProxyAwareRemoteAddrElement implements AccessLogElement {
        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {

            Enumeration<String> headers = request.getHeaders("X-Forwarded-For");
            if (headers.hasMoreElements()) {
                String[] ips = headers.nextElement().split(", "); // If multiple X-Forwarded-For headers, take first
                if (ips.length > 0 && !ips[0].equals("127.0.0.1")) {
                    buf.append(handleBlankSafely(ips[0])); // If multiple IPs, remote is the first (rest are proxies)
                    return;
                }
            }

            if (requestAttributesEnabled) {
                String addr = (String) request.getAttribute(REMOTE_ADDR_ATTRIBUTE);
                if (addr == null) {
                    buf.append(handleBlankSafely(request.getRemoteAddr()));
                } else {
                    buf.append(handleBlankSafely(addr));
                }
            } else {
                buf.append(handleBlankSafely(request.getRemoteAddr()));
            }
        }
    }

    /**
     * A new element - returns the request's body,
     * Base64-URL-safe-encoded.
     */
    protected static class Base64EncodedBodyElement implements AccessLogElement {
        @Override
        public void addElement(StringBuilder buf, Date date, Request request,
                Response response, long time) {

            buf.append(readBodyFromAttribute(request));
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
     * Base64-URL-safe encodes a string or returns a
     * "-" if not possible.
     *
     * @param b The byte array to encode
     * @return The encoded string, or "-" if not possible
     */
    protected static String base64EncodeSafely(byte[] b) {
        try {
            return Base64.encodeBase64URLSafeString(b);
        } catch (Exception e) {
            return "-";
        }
    }

    /**
     * Encodes a string or returns a "-" if not possible.
     *
     * @param s The String to encode
     * @return The encoded string
     */
    protected static String uriEncodeSafely(String s) {
        try {
            return URLEncoder.encode(s, ProjectSettings.DEFAULT_ENCODING);
        } catch (Exception e) {
            return "-";
        }
    }

    /**
     * Reads the body from an attribute on the request.
     * We assume this attribute has been set by the
     * BodyRequestWrapper, which was pulled in by the
     * SnowplowBodyFilter.
     *
     * @param request The request
     * @return The request's body
     */
    protected static String readBodyFromAttribute(Request request) {

        Object body = request.getAttribute(ProjectSettings.BODY_ATTRIBUTE);
        if (body == null) {
            return "-";
        }

        try {
            String str = (String) body;
            if (str.equals("")) {
                return "-";
            } else {
                return base64EncodeSafely(str.getBytes(ProjectSettings.DEFAULT_ENCODING));
            }
        } catch (Exception e) {
            return "-";
        }
    }
}
