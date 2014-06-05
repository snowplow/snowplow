# Java Analytics for Snowplow

## Overview

Add analytics to your Java programs and data pipelines with the [Snowplow] [snowplow] event tracker for [Java] [java].

## Quick Start

### Downloads

There are a few ways to get the Java tracker started. Since production is still early and large changes are being made daily, I have not uploaded any jar files yet. Class files can be found in the out folder. At the moment the source files are set up as a package.

To get started, download the javaplow package and install the three library dependencies [Here] [dependencies].

Place the javaplow package in your projects src folder or root. If performed correctly you should be able to compile and ready to instantiate a tracker.

### Set up the Tracker

Documentation on functions are available [here][documentation]. 

To instantiate a tracker in your code (can be global or local to the process being tracked) simply instantiate the `Tracker` interface with one of the following:

    TrackerC(String collector_URI, String namespace)

    TrackerC(String collector_uri, String namespace, String app_id, String context_vendor, boolean base64_encode, boolean enable_contracts)

e.g.

    Tracker t1 = new TrackerC("d3rkrsqld9gmqf.cloudfront.net", "Snowplow Java Tracker Test", "testing_app", "com.snowplow", true, true);

There are more options you can configure, which will be documented soon. 

    t1.setUserID("Kevin"); 
    t1.setLanguage("eng");
    t1.setPlatform("cnsl");
    t1.setScreenResolution(1260, 1080);
    t1.track();

----

Now, locally in the function you would like to track, place a tracking call to one of the supported methods. Currently implemented are:

    t1.track_page_view(String page_url, String page_title, String referrer, String context)

    t1.track_struct_event(String category, String action, String label, String property, int value, String vendor, String context)

    t1.track_unstruct_event(String eventVendor, String eventName, String dictInfo, String context)

    t1.track_screen_view(String name, String id, String context)

    t1.track_ecommerce_transaction_item(String order_id, String sku, Double price, Integer quantity, String name, String category, String currency, String context, String transaction_id) *

    t1.track_ecommerce_transaction(String order_id, Double total_value, String affiliation, Double tax_value,Double shipping, String city, String state, String country, String currency, List<Map<String, String>> items, String context) *

###### * Tested and succeeded, but not as much as other functions. Potentially contains bugs.

And thats it! When the tracking function is called, all the information gathered will be sent to the collector URI you have set!

Now you're ready to [view the documentation][documentation]!

Dont have the collector configured? [See Snowplow setup] [setup]

#### Don't need to fill in every field?

A few fields are required, like collector_uri and namespace, or page_url or catrgory and action etc. For the most part, if you dont need a field you can use null! The code will convert those values whether they be number or letters to empty strings which in turn will not show up in the final database.

## Comments

- For a full list of available functions, look into the packages interfaces.
- Context is meant to be in JSON String format

    `String i = "{'Movie':'Shawshank Redemption', 'Time':'100 Minutes' }"`

## Find out more

| Technical Docs                  | Setup Guide               | Roadmap                 | Contributing                      |
|---------------------------------|---------------------------|-------------------------|-----------------------------------|
| ![i1] [techdocs-image]          | ![i2] [setup-image]       | ![i3] [roadmap-image]   | ![i4] [contributing-image]        |
| **[Technical Docs] [techdocs]** | **[Setup Guide] [setup]** | **[Roadmap] [roadmap]** | **[Contributing] [contributing]** |

## Copyright and license

The Snowplow Java Tracker is copyright 2013 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[java]: http://www.java.com/en/

[snowplow]: http://snowplowanalytics.com

[dependencies]: https://drive.google.com/folderview?id=0B9v7AAtH8DSpWWZ1c3RUZjU3WlU&usp=sharing
[documentation]: https://gleasonk.github.io/Saggezza/JavaDoc/index.html

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/Snowplow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-Snowplow
[roadmap]: https://github.com/snowplow/snowplow/wiki
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

[license]: http://www.apache.org/licenses/LICENSE-2.0
