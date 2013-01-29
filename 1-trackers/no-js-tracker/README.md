# The SnowPlow No-Javascript tracker (pixel tracker)

The SnowPlow No-Javascript tracker can be used to track views of HTML pages that do not support Javascript. Examples include:

* HTML emails
* Pages hosted on 3rd party websites (e.g. READMEs on Github)
* Product listings on 3rd party marketplaces (e.g. eBay)

The No-JS tracker is effectively a wizard that generates a static SnowPlow tracking tag for a particular HTML page e.g. email. The wizard takes a set of inputs e.g. collector endpoint and page title, and generates a tracking tag that works with any of the SnowPlow collectors.

The wizard source code can be found [here] [wizard]. The logic for generating the tag is stored in the [Javascript file] [js-wizard] invoked by the wizard.

For more details about how to use the No-JS tracker see [this introductory blog post] [intro-blog-post]. A hosted version of the wizard can be found [on the SnowPlow Analytics website] [hosted-wizard].

[wizard]: https://github.com/snowplow/snowplow/blob/master/1-trackers/no-js-tracker/html/no-js-embed-code-generator.html
[js-wizard]: https://github.com/snowplow/snowplow/blob/master/1-trackers/no-js-tracker/js/no-js-tracker.js
[intro-blog-post]: http://snowplowanalytics.com/blog/2013/01/29/introducing-the-no-js-tracker/
[hosted-wizard]: http://snowplowanalytics.com/no-js-tracker.html