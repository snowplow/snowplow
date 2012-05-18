# snowplow.js: JavaScript tracking for SnowPlow

## Introduction

`snowplow.js` (or `sp.js` in minified form) is the JavaScript page and event tracking library for [SnowPlow] [snowplow].

`snowplow.js` is largely based on Anthon Pang's excellent [`piwik.js`] [piwikjs], the JavaScript tracker for the open-source [Piwik] [piwik] project, and is distributed under the same license ([Simplified BSD] [bsd]). For completeness, the main differences between `snowplow.js` and `piwik.js` are documented below.

## Contents

Contents of this folder are as follows:

* In this folder is this README and the [Simplified BSD] [bsd] license
* `js` contains the un-minified JavaScript (`snowplow.js`), the minified JavaScript (`sp.js`) and a Bash script for minifying (`snowpak.sh`)
* `static` contains the 1x1 transparent pixel (`ice.png`) which is fetched by the JavaScript from a CloudFront distribution

## Documentation

Besides this README, there are two main SnowPlow guides which relate to `snowplow.js`:

* [Integrating snowplow.js into your site] [integrating]
* [Self-hosting SnowPlow] [selfhosting]

## Main differences between snowplow.js and piwik.js

The main differences today are as follows:

* Simplified the set of querystring name-value pairs (removing Piwik-specific values and values which CloudFront logging gives us for free)
* Tracking is now configured with an account ID (CloudFront subdomain) rather than a full tracker URL
* Added new `trackEvent` functionality
* Added new `trackImpression` functionality
* Added browser language and visit ID to the querystring (because not available via CloudFront logging)
* Removed `POST` functionality (because S3 logging does not support `POST`)
* Removed goal tracking functionality
* Removed custom variables
* Removed ecommerce tracking functionality (ecommerce tracking is now handled by events)
* Removed `piwik.js`'s own deprecated 'legacy' functionality

We expect these two scripts to diverge further as we continue to evolve SnowPlow (see the next section for more detail).

## Roadmap

`piwik.js` provides an excellent starting point for `snowplow.js` - and we would encourage any SnowPlow users to check out [Piwik] [piwik] as an open-source alternative to using Google Analytics.

However, we fully expect `snowplow.js` to diverge from `piwik.js`, for three main reasons:

* **Tracking technology:** there are some differences in what is advisable/possible using a PHP tracker (like Piwik) versus using an Amazon S3 pixel (like SnowPlow)
* **Approach to data aggregation:** Piwik performs 'pre-aggregation' on the incoming data in both `piwik.js` and the [PHP tracker] [piwikphp] prior to logging to database. The SnowPlow approach is to defer all such aggregations to the MapReduce phase - which should reduce the complexity of `snowplow.js`
* **Philosophy on premature analysis:** Piwik follows the 'classical' model of web analytics, where the sensible analyses are agreed in advance, formalised by being integrated into the site (e.g. by tracking goals and conversion funnels) and then analysed. SnowPlow views this as 'premature analysis', and encourages logging lots of intent-agnostic events and then figuring out what they mean later

Planned items on the roadmap are as follows:

* Remove site ID functionality
* Remove unused campaign marketing variable code (as no longer used)
* Rewrite in CoffeeScript (joke!)

## Copyright and license

Significant portions of `snowplow.js` copyright 2010 Anthon Pang. Remainder copyright 2012 Orderly Ltd.

Licensed under the [Simplified BSD] [bsd] license.

[snowplow]: http://www.keplarllp.com/blog/2012/02/introducing-snowplow-the-worlds-most-powerful-web-analytics-platform
[piwik]: http://piwik.org/
[piwikjs]: https://github.com/piwik/piwik/blob/master/js/piwik.js
[piwikphp]: https://github.com/piwik/piwik/blob/master/piwik.php
[bsd]: http://www.opensource.org/licenses/bsd-license.php 
[integrating]: /snowplow/snowplow/blob/master/docs/03_integrating_snowplowjs.md
[selfhosting]: /snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md