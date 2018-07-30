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
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.Enumeration;
import java.util.Iterator;
import java.net.URLEncoder;

// Apache Logging
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

// Apache Commons
import org.apache.commons.codec.binary.Base64;

// Tomcat, Catalina and Coyote
import org.apache.catalina.LifecycleException;
import org.apache.catalina.valves.AbstractAccessLogValve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.tomcat.util.buf.ByteChunk;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.buf.B2CConverter;

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
 * This file was created by:
 * 1. Extending AbstractAccessLogValve with the functionality we require: http://grepcode.com/file/repo1.maven.org/maven2/org.apache.tomcat/tomcat-catalina/8.0.11/org/apache/catalina/valves/AbstractAccessLogValve.java
 * 2. Adding in the *full* contents of http://grepcode.com/file/repo1.maven.org/maven2/org.apache.tomcat/tomcat-catalina/8.0.11/org/apache/catalina/valves/AccessLogValve.java
 * 
 */
public class SnowplowAccessLogValve extends AbstractAccessLogValve {

    private static final Log log = LogFactory.getLog(SnowplowAccessLogValve.class);

    //------------------------------------------------------ Constructor
    public SnowplowAccessLogValve() {
        super();
    }

    // ----------------------------------------------------- Instance Variables


    /**
     * The as-of date for the currently open log file, or a zero-length
     * string if there is no open log file.
     */
    private volatile String dateStamp = "";


    /**
     * The directory in which log files are created.
     */
    private String directory = "logs";

    /**
     * The prefix that is added to log file filenames.
     */
    protected String prefix = "access_log";


    /**
     * Should we rotate our log file? Default is true (like old behavior)
     */
    protected boolean rotatable = true;

    /**
     * Should we defer inclusion of the date stamp in the file
     * name until rotate time? Default is false.
     */
    protected boolean renameOnRotate = false;


    /**
     * Buffered logging.
     */
    private boolean buffered = true;


    /**
     * The suffix that is added to log file filenames.
     */
    protected String suffix = "";


    /**
     * The PrintWriter to which we are currently logging, if any.
     */
    protected PrintWriter writer = null;


    /**
     * A date formatter to format a Date using the format
     * given by <code>fileDateFormat</code>.
     */
    protected SimpleDateFormat fileDateFormatter = null;


    /**
     * The current log file we are writing to. Helpful when checkExists
     * is true.
     */
    protected File currentLogFile = null;

    /**
     * Instant when the log daily rotation was last checked.
     */
    private volatile long rotationLastChecked = 0L;

    /**
     * Do we check for log file existence? Helpful if an external
     * agent renames the log file so we can automagically recreate it.
     */
    private boolean checkExists = false;

    /**
     * Date format to place in log file name.
     */
    protected String fileDateFormat = ".yyyy-MM-dd";

    /**
     * Character set used by the log file. If it is <code>null</code>, the
     * system default character set will be used. An empty string will be
     * treated as <code>null</code> when this property is assigned.
     */
    protected String encoding = ProjectSettings.DEFAULT_ENCODING;

    // ------------------------------------------------------------- Properties


    /**
     * Return the directory in which we create log files.
     */
    public String getDirectory() {
        return (directory);
    }


    /**
     * Set the directory in which we create log files.
     *
     * @param directory The new log file directory
     */
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    /**
     * Check for file existence before logging.
     */
    public boolean isCheckExists() {

        return checkExists;

    }


    /**
     * Set whether to check for log file existence before logging.
     *
     * @param checkExists true meaning to check for file existence.
     */
    public void setCheckExists(boolean checkExists) {

        this.checkExists = checkExists;

    }


    /**
     * Return the log file prefix.
     */
    public String getPrefix() {
        return (prefix);
    }


    /**
     * Set the log file prefix.
     *
     * @param prefix The new log file prefix
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }


    /**
     * Should we rotate the logs
     */
    public boolean isRotatable() {
        return rotatable;
    }


    /**
     * Set the value is we should we rotate the logs
     *
     * @param rotatable true is we should rotate.
     */
    public void setRotatable(boolean rotatable) {
        this.rotatable = rotatable;
    }


    /**
     * Should we defer inclusion of the date stamp in the file
     * name until rotate time
     */
    public boolean isRenameOnRotate() {
        return renameOnRotate;
    }


    /**
     * Set the value if we should defer inclusion of the date
     * stamp in the file name until rotate time
     *
     * @param renameOnRotate true if defer inclusion of date stamp
     */
    public void setRenameOnRotate(boolean renameOnRotate) {
        this.renameOnRotate = renameOnRotate;
    }


    /**
     * Is the logging buffered
     */
    public boolean isBuffered() {
        return buffered;
    }


    /**
     * Set the value if the logging should be buffered
     *
     * @param buffered true if buffered.
     */
    public void setBuffered(boolean buffered) {
        this.buffered = buffered;
    }


    /**
     * Return the log file suffix.
     */
    public String getSuffix() {
        return (suffix);
    }


    /**
     * Set the log file suffix.
     *
     * @param suffix The new log file suffix
     */
    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    /**
     *  Return the date format date based log rotation.
     */
    public String getFileDateFormat() {
        return fileDateFormat;
    }


    /**
     *  Set the date format date based log rotation.
     */
    public void setFileDateFormat(String fileDateFormat) {
        String newFormat;
        if (fileDateFormat == null) {
            newFormat = "";
        } else {
            newFormat = fileDateFormat;
        }
        this.fileDateFormat = newFormat;

        synchronized (this) {
            fileDateFormatter = new SimpleDateFormat(newFormat, Locale.US);
            fileDateFormatter.setTimeZone(TimeZone.getDefault());
        }
    }

    /**
     * Return the character set name that is used to write the log file.
     *
     * @return Character set name, or <code>null</code> if the system default
     *  character set is used.
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Set the character set that is used to write the log file.
     *
     * @param encoding The name of the character set.
     */
    public void setEncoding(String encoding) {
        if (encoding != null && encoding.length() > 0) {
            this.encoding = encoding;
        }
    }

    // --------------------------------------------------------- Public Methods

    /**
     * Execute a periodic task, such as reloading, etc. This method will be
     * invoked inside the classloading context of this container. Unexpected
     * throwables will be caught and logged.
     */
    @Override
    public synchronized void backgroundProcess() {
        if (getState().isAvailable() && getEnabled() && writer != null &&
                buffered) {
            writer.flush();
        }
    }

    /**
     * Rotate the log file if necessary.
     */
    public void rotate() {
        if (rotatable) {
            // Only do a logfile switch check once a second, max.
            long systime = System.currentTimeMillis();
            if ((systime - rotationLastChecked) > 1000) {
                synchronized(this) {
                    if ((systime - rotationLastChecked) > 1000) {
                        rotationLastChecked = systime;

                        String tsDate;
                        // Check for a change of date
                        tsDate = fileDateFormatter.format(new Date(systime));

                        // If the date has changed, switch log files
                        if (!dateStamp.equals(tsDate)) {
                            close(true);
                            dateStamp = tsDate;
                            open();
                        }
                    }
                }
            }
        }
    }

    /**
     * Rename the existing log file to something else. Then open the
     * old log file name up once again. Intended to be called by a JMX
     * agent.
     *
     *
     * @param newFileName The file name to move the log file entry to
     * @return true if a file was rotated with no error
     */
    public synchronized boolean rotate(String newFileName) {

        if (currentLogFile != null) {
            File holder = currentLogFile;
            close(false);
            try {
                holder.renameTo(new File(newFileName));
            } catch (Throwable e) {
                ExceptionUtils.handleThrowable(e);
                log.error(sm.getString("accessLogValve.rotateFail"), e);
            }

            /* Make sure date is correct */
            dateStamp = fileDateFormatter.format(
                    new Date(System.currentTimeMillis()));

            open();
            return true;
        } else {
            return false;
        }

    }

    // -------------------------------------------------------- Private Methods


    /**
     * Create a File object based on the current log file name.
     * Directories are created as needed but the underlying file
     * is not created or opened.
     *
     * @param useDateStamp include the timestamp in the file name.
     * @return the log file object
     */
    private File getLogFile(boolean useDateStamp) {

        // Create the directory if necessary
        File dir = new File(directory);
        if (!dir.isAbsolute()) {
            dir = new File(getContainer().getCatalinaBase(), directory);
        }
        if (!dir.mkdirs() && !dir.isDirectory()) {
            log.error(sm.getString("accessLogValve.openDirFail", dir));
        }

        // Calculate the current log file name
        File pathname;
        if (useDateStamp) {
            pathname = new File(dir.getAbsoluteFile(), prefix + dateStamp
                    + suffix);
        } else {
            pathname = new File(dir.getAbsoluteFile(), prefix + suffix);
        }
        File parent = pathname.getParentFile();
        if (!parent.mkdirs() && !parent.isDirectory()) {
            log.error(sm.getString("accessLogValve.openDirFail", parent));
        }
        return pathname;
    }

    /**
     * Move a current but rotated log file back to the unrotated
     * one. Needed if date stamp inclusion is deferred to rotation
     * time.
     */
    private void restore() {
        File newLogFile = getLogFile(false);
        File rotatedLogFile = getLogFile(true);
        if (rotatedLogFile.exists() && !newLogFile.exists() &&
            !rotatedLogFile.equals(newLogFile)) {
            try {
                if (!rotatedLogFile.renameTo(newLogFile)) {
                    log.error(sm.getString("accessLogValve.renameFail", rotatedLogFile, newLogFile));
                }
            } catch (Throwable e) {
                ExceptionUtils.handleThrowable(e);
                log.error(sm.getString("accessLogValve.renameFail", rotatedLogFile, newLogFile), e);
            }
        }
    }


    /**
     * Close the currently open log file (if any)
     *
     * @param rename Rename file to final name after closing
     */
    private synchronized void close(boolean rename) {
        if (writer == null) {
            return;
        }
        writer.flush();
        writer.close();
        if (rename && renameOnRotate) {
            File newLogFile = getLogFile(true);
            if (!newLogFile.exists()) {
                try {
                    if (!currentLogFile.renameTo(newLogFile)) {
                        log.error(sm.getString("accessLogValve.renameFail", currentLogFile, newLogFile));
                    }
                } catch (Throwable e) {
                    ExceptionUtils.handleThrowable(e);
                    log.error(sm.getString("accessLogValve.renameFail", currentLogFile, newLogFile), e);
                }
            } else {
                log.error(sm.getString("accessLogValve.alreadyExists", currentLogFile, newLogFile));
            }
        }
        writer = null;
        dateStamp = "";
        currentLogFile = null;
    }


    /**
     * Log the specified message to the log file, switching files if the date
     * has changed since the previous log call.
     *
     * @param message Message to be logged
     */
    @Override
    public void log(CharArrayWriter message) {

        rotate();

        /* In case something external rotated the file instead */
        if (checkExists) {
            synchronized (this) {
                if (currentLogFile != null && !currentLogFile.exists()) {
                    try {
                        close(false);
                    } catch (Throwable e) {
                        ExceptionUtils.handleThrowable(e);
                        log.info(sm.getString("accessLogValve.closeFail"), e);
                    }

                    /* Make sure date is correct */
                    dateStamp = fileDateFormatter.format(
                            new Date(System.currentTimeMillis()));

                    open();
                }
            }
        }

        // Log this message
        try {
            synchronized(this) {
                if (writer != null) {
                    message.writeTo(writer);
                    writer.println("");
                    if (!buffered) {
                        writer.flush();
                    }
                }
            }
        } catch (IOException ioe) {
            log.warn(sm.getString(
                    "accessLogValve.writeFail", message.toString()), ioe);
        }
    }


    /**
     * Open the new log file for the date specified by <code>dateStamp</code>.
     */
    protected synchronized void open() {
        // Open the current log file
        // If no rotate - no need for dateStamp in fileName
        File pathname = getLogFile(rotatable && !renameOnRotate);

        Charset charset = null;
        if (encoding != null) {
            try {
                charset = B2CConverter.getCharset(encoding);
            } catch (UnsupportedEncodingException ex) {
                log.error(sm.getString(
                        "accessLogValve.unsupportedEncoding", encoding), ex);
            }
        }
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }

        try {
            writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(pathname, true), charset), 128000),
                    false);

            currentLogFile = pathname;
        } catch (IOException e) {
            writer = null;
            currentLogFile = null;
            log.error(sm.getString("accessLogValve.openFail", pathname), e);
        }
    }

    /**
     * Start this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void startInternal() throws LifecycleException {

        // Initialize the Date formatters
        String format = getFileDateFormat();
        fileDateFormatter = new SimpleDateFormat(format, Locale.US);
        fileDateFormatter.setTimeZone(TimeZone.getDefault());
        dateStamp = fileDateFormatter.format(new Date(System.currentTimeMillis()));
        if (rotatable && renameOnRotate) {
            restore();
        }
        open();

        super.startInternal();
    }


    /**
     * Stop this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void stopInternal() throws LifecycleException {

        super.stopInternal();
        close(false);
    }

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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
        public void addElement(CharArrayWriter buf, Date date, Request request,
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
