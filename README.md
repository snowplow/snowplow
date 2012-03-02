# snowplow.js: JavaScript tracking for SnowPlow

## Introducing snowplow.js

`snowplow.js` (or `sp.js` in minified form) is the JavaScript page and event tracking library for [SnowPlow] [snowplow].

`snowplow.js` is largely based on Anton Pang's excellent [`piwik.js`] [piwikjs], the JavaScript tracker for the open-source [Piwik] [piwik] project, and is distributed under the same license ([Simplified BSD] [bsd]). For completeness, the main differences between `snowplow.js` and `piwik.js` are documented below.

## Documentation

Besides this README, there are two main instruction guides written for `snowplow.js`:

* Integrating `snowplow.js` into your site
* Bundling `snowplow.js` into your JavaScript

TODO: write these guides!

## Main differences between snowplow.js and piwik.js

The main differences today are as follows:

* Simplified the set of querystring name-value pairs (removing Piwik-specific values and values which Amazon S3 logging gives us for free)
* Added new `trackEvent` functionality
* Removed `POST` functionality (because S3 logging does not support `POST`)
* Removed `piwik.js`'s own deprecated 'legacy' functionality

We expect these two scripts to diverge further as we continue to evolve SnowPlow (see the next section for more detail).

## Roadmap

`piwik.js` provides an excellent starting point for `snowplow.js` - and we would encourage any SnowPlow users to check out [Piwik] [piwik] as an open-source alternative to using Google Analytics.

However, we fully expect `snowplow.js` to diverge from `piwik.js`, for three main reasons:

* **Tracking technology:** there are some differences in what is advisable/possible using a PHP tracker (like Piwik) versus using an Amazon S3 pixel (like SnowPlow)
* **Approach to data aggregation:** Piwik performs 'pre-aggregation' on the incoming data in both `piwik.js` and the [PHP tracker] [piwikphp] prior to logging to database. The SnowPlow approach is to defer all such aggregations to the MapReduce phase - which should reduce the complexity of `snowplow.js`
* **Philosophy on premature analysis:** Piwik follows the 'classical' model of web analytics, where the sensible analyses are agreed in advance, formalised by being integrated into the site (e.g. by tracking goals and conversion funnels) and then analysed. SnowPlow views this as 'premature analysis', and encourages logging lots of intent-agnostic events and then figuring out what they mean later

Planned items on the roadmap are as follows:

* Remove goal tracking functionality
* Update ecommerce tracking functionality to become event-based (e.g. support for `removeEcommerceItem`)
* Rewrite in CoffeeScript (joke!)

## Copyright and license

Significant portions copyright 2010 Anton Pang. Remainder copyright 2012 Orderly Ltd.

Licensed under the [Simplified BSD] [bsd] license.

[snowplow]: http://www.keplarllp.com/blog/2012/02/introducing-snowplow-the-worlds-most-powerful-web-analytics-platform
[piwik]: http://piwik.org/
[piwikjs]: https://github.com/piwik/piwik/blob/master/js/piwik.js
[piwikphp]: https://github.com/piwik/piwik/blob/master/piwik.php
[bsd]: http://www.opensource.org/licenses/bsd-license.php 