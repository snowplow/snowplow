# The Cloudfront collector

## Introduction

The Cloudfront collector is the simplest SnowPlow collector (because it has no moving parts) and most popular SnowPlow collector. It is incredibly robust and scalable, by leveraging Amazon's cloud infrastructure.

## How it works, in a nutshell

The SnowPlow tracking pixel is served from Cloudfront. The SnowPlow tracker requests the pixel (using a `GET`), and appends the data to be logged in SnowPlow in the query string for the `GET` request. Amazon provides Cloudfront logging: the request (incl. the query string) gets logged to S3 including some additional data provided by Cloudfront. (E.g. requester IP address and URL.) These can then be parsed by the ETL module.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-the-Cloudfront-collector
[techdocs]: https://github.com/snowplow/snowplow/wiki/cloudfront-collector
[wiki]: https://github.com/snowplow/snowplow/wiki
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png