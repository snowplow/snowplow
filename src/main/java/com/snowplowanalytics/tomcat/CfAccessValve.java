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

import org.apache.catalina.valves.AccessLogValve;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.MatchResult;

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

    @Override
    public void log(String message) {
        super.log(sanitize(message));
    }
}