# The SnowPlow No-JavaScript tracker (pixel tracker)

The SnowPlow No-JavaScript (No-JS) tracker can be used to track views of HTML pages that do not support JavaScript. Examples include:

* HTML emails
* Pages hosted on 3rd party websites (e.g. READMEs on Github)
* Product listings on 3rd party marketplaces (e.g. eBay)

The No-JavaScript tracker is effectively a wizard that generates a static SnowPlow tracking tag for a particular HTML page e.g. email. The wizard takes a set of inputs e.g. collector endpoint and page title, and generates a tracking tag that works with any of the SnowPlow collectors.

The wizard source code can be found [here][wizard]. The logic for generating the tag is stored in the [Javascript file][js-wizard] invoked by the wizard.

## Find out more

| Technical Docs             | Setup Guide          |
|----------------------------|----------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] |

## Copyright and license

The No-JavaScript Tracker is copyright 2012-2013 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[wizard]: https://github.com/snowplow/snowplow/blob/master/1-trackers/no-js/html/no-js-embed-code-generator.html
[js-wizard]: https://github.com/snowplow/snowplow/blob/master/1-trackers/no-js/js/no-js-tracker.js
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[techdocs]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/pixel-tracker/
[setup]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/pixel-tracker/#setting-up-the-pixel-tracker
[license]: http://www.apache.org/licenses/LICENSE-2.0
