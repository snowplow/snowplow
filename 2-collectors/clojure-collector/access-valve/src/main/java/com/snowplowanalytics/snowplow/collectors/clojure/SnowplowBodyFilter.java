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
import java.io.IOException;
 
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

// Apache
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

// This project
import com.snowplowanalytics.snowplow.collectors.clojure.BodyRequestWrapper;
import com.snowplowanalytics.snowplow.collectors.clojure.generated.ProjectSettings;

/**
 * Sourced from:
 *
 * - http://www.coderanch.com/t/364591/Servlets/java/read-request-body-filter
 * - http://natch3z.blogspot.co.uk/2009/01/read-request-body-in-filter.html
 */
public class SnowplowBodyFilter implements Filter {

    private static Log log = LogFactory.getLog(SnowplowBodyFilter.class);  

    public void doFilter (ServletRequest servletRequest,   
                          ServletResponse servletResponse,   
                          FilterChain filterChain)  
        throws IOException, ServletException {  
              
        // Perform request filtering  
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;  
        BodyRequestWrapper bodyRequestWrapper;  
        try {  
            bodyRequestWrapper = new BodyRequestWrapper(httpServletRequest);
            httpServletRequest.setAttribute(ProjectSettings.BODY_ATTRIBUTE, bodyRequestWrapper.getBody());
        } catch (Exception e) {  
            log.warn("Unable to wrap the request", e);  
            throw new ServletException("Unable to wrap the request", e);              
        }
          
        // Continue the filter chain  
        filterChain.doFilter(bodyRequestWrapper, servletResponse);  
          
        // Perform response filtering
    }  

    public void init(FilterConfig config) throws ServletException {
        // No code to initialize the filter
    }

    public void destroy() {
        // No code to release any resource
    }

}
