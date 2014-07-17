# tomcat-cf-access-log-valve

Custom `AccessLogValve` for Tomcat 7 to help generate CloudFront-like access logs, e.g. on Amazon Elastic Beanstalk.

Used by the SnowPlow Clojure Collector.

Compared to the standard `AccessLogValve`, this valve:

1. Introduces a new pattern, 'I', to escape an incoming header
2. Introduces a new pattern, 'C', to fetch a cookie stored on the response
3. Re-implements the pattern 'i' to ensure that "" (empty string) is replaced with "-"
4. Re-implements the pattern 'q' to remove the "?" and ensure "" (empty string) is replaced with "-"
5. Overwrites the 'v' pattern, to write the version of this AccessLogValve, rather than the local server name

To build:

    $ git clone git@github.com:snowplow/tomcat-cf-access-log-valve.git
    $ cd tomcat-cf-access-log-valve
    $ sbt assembly

