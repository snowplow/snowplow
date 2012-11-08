# The Cloudfront collector

The Cloudfront collector is the simplest SnowPlow collector (because it has no moving parts). For this reason, there is not much source code here! (The only files are the actual tracking pixel served from Cloudfront.)

This collector is, however, incredibly robust and scalable, by leveraging Amazon's cloud infrastructure.

## How it works, in a nutshell

The SnowPlow tracking pixel is served from Cloudfront. The SnowPlow tracker requests the pixel (using a `GET`), and appends the data to be logged in SnowPlow in the query string for the `GET` request. Amazon provides Cloudfront logging: the request (incl. the query string) gets logged to S3 including some additional data provided by Cloudfront. (E.g. requester IP address and URL.) These can then be parsed by the ETL module.

## Documentation

1. [Setting up the Cloudfront collector] [setup]: a step-by-step guide to setting up Cloudfront to use with SnowPlow
2. [Technical documentation] [tech-docs] include details of the Cloudfront log format

Both sets of documents are published on the [wiki].

[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-the-cloudfront-collector
[tech-docs]: https://github.com/snowplow/snowplow/wiki/collectors
[wiki]: https://github.com/snowplow/snowplow/wiki