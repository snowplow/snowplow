package com.snowplowanalytics.snowplow.kinesis.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/*
 * This class helps to keep some private settings outside of *.properties files
 * by allowing environment variables to be loaded in place of the *.properties
 * values with similar syntax to sbt from scala: ${ENV_VAR_NAME}
 */
public class PropertiesUtils {

	/*
     * Returns input string with environment variable references expanded, e.g. $SOME_VAR or ${SOME_VAR}
     * http://stackoverflow.com/a/9725352/2256243
     */
    public static String resolveEnvVars(String input)
    {
        if (null == input)
        {
            return null;
        }
        // match ${ENV_VAR_NAME} or $ENV_VAR_NAME
        Pattern p = Pattern.compile("\\$\\{(\\w+)\\}");
        Matcher m = p.matcher(input); // get a matcher object
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
            String envVarValue = System.getenv(envVarName);
            m.appendReplacement(sb, null == envVarValue ? "" : envVarValue);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    // http://stackoverflow.com/a/16027293/2256243
    public static InputStream parseConfigStream(InputStream stream) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        try {
            StringBuilder sb = new StringBuilder();
            String line = resolveEnvVars(br.readLine());

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = resolveEnvVars(br.readLine());
            }
            String configString = sb.toString();
            InputStream configStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));
            return configStream;
        } finally {
            br.close();
        }
    }
}
