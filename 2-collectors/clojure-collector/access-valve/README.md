# tomcat-cf-access-log-valve

Custom `AccessLogValve` for Tomcat 7 to help generate CloudFront-like access logs, e.g. on Amazon Elastic Beanstalk.

Used by the SnowPlow Clojure Collector.

Compared to the standard `AccessLogValve`, this valve:

1. Introduces a new pattern, 'I', to escape an incoming header
2. Introduces a new pattern, 'C', to fetch a cookie stored on the response
3. Re-implements the pattern 'i' to ensure that "" (empty string) is replaced with "-"
4. Re-implements the pattern 'q' to remove the "?" and ensure "" (empty string) is replaced with "-"
5. Overwrites the 'v' pattern, to write the version of this AccessLogValve, rather than the local server name
6. Introduces a new pattern, 'w' to capture the request's body
7. Introduces a new pattern, '~' to capture the request's content type
8. Re-implements the pattern 'a' to get remote IP more reliably, even through proxies

To build:

    $ git clone git@github.com:snowplow/tomcat-cf-access-log-valve.git
    $ cd tomcat-cf-access-log-valve
    $ gradle jar

When you are ready to build an updated servlet:

    $ gradle installFatJarInServlet
