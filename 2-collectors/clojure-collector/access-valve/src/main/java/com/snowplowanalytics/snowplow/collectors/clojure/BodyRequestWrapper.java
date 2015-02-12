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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.ServletInputStream;
import javax.servlet.ReadListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

// Apache
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

// This project
import com.snowplowanalytics.snowplow.collectors.clojure.generated.ProjectSettings;

/** 
 * Class which is used to wrap a request in order that the wrapped request's input stream can be  
 * read once and later be read again in a pseudo fashion by virtue of keeping the original payload 
 * as a string to be returned by subsequent calls to getInputStream().
 *
 * Sourced from:
 *
 * - http://www.coderanch.com/t/364591/Servlets/java/read-request-body-filter
 * - http://natch3z.blogspot.co.uk/2009/01/read-request-body-in-filter.html
 */  
public class BodyRequestWrapper extends HttpServletRequestWrapper {  
      
    private static Log log = LogFactory.getLog(BodyRequestWrapper.class);  

    private final String body;

    /**
     * Constructor for the BodyRequestWrapper.
     * We read the request's body (aka InputStream) into a
     * body, and also set it as an attribute. The
     * body will be used in the future when the
     * input stream is requested by the Clojure Collector.
     */
    public BodyRequestWrapper(HttpServletRequest request) {  
          
        super(request);

        // Read the original payload into the jsonPayload variable  
        StringBuilder stringBuilder = new StringBuilder();  
        BufferedReader bufferedReader = null;  
        try {  
            // Read the payload into the StringBuilder  
            InputStream inputStream = request.getInputStream();  

            String charset = request.getCharacterEncoding();
            if (charset == null) charset = ProjectSettings.DEFAULT_ENCODING;

            if (inputStream != null) {  
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset));  
                char[] charBuffer = new char[128];  
                int bytesRead = -1;  
                while ((bytesRead = bufferedReader.read(charBuffer)) > 0) {  
                    stringBuilder.append(charBuffer, 0, bytesRead);  
                }  
            } else {  
                // Make an empty string since there is no payload  
                stringBuilder.append("");  
            }  
        } catch (IOException ex) {  
            log.error("Error reading the request payload", ex);
        } finally {  
            if (bufferedReader != null) {  
                try {  
                    bufferedReader.close();  
                } catch (IOException iox) {  
                    // Ignore  
                }  
            }  
        }  
        
        body = stringBuilder.toString();
    }

    @Override
    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(this.getInputStream()));
    }

    public String getBody() {
        return this.body;
    }

    /** 
     * Override of the getInputStream() method which returns an InputStream that reads from the 
     * stored body String instead of from the request's actual InputStream.
     *
     * Note that the Clojure Collector doesn't actually do anything with the input stream, but
     * it still needs to be available for reading (so mustn't throw an IOException).
     *
     * TODO: see if we can drop the body to save some performance without blowing up
     * the Clojure Collector.
     */  
    @Override  
    public ServletInputStream getInputStream() throws IOException {  
          
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body.getBytes());  
        ServletInputStream inputStream = new ServletInputStream() {  
            public int read()   
                throws IOException {  
                return byteArrayInputStream.read();  
            }
            @Override
            public boolean isFinished() {
                return byteArrayInputStream.available() == 0;
            }
            @Override
            public boolean isReady() {
                return byteArrayInputStream.available() > 0;
            }
            @Override
            public void setReadListener(ReadListener readListener) {
                throw new UnsupportedOperationException("setReadListener");
            }
        };  
        return inputStream;  
    }
}
