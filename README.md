tomcat-cf-access-valve
======================

Custom AccessLogValve for Tomcat to help generate CloudFront-like access logs, e.g. on Amazon Elastic Beanstalk.

Used by the SnowPlow Clojure Collector.

To build:

    $ git clone git@github.com:snowplow/tomcat-cf-access-log-valve.git
    $ cd tomcat-cf-access-log-valve
    $ sbt assembly

