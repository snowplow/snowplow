# Integrating snowplow.js into your site

## Table of Contents

1. [Introduction](#intro)
2. [Asynchronous integration](#async)
3. [Synchronous integration](#sync)
4. [Event tracking](#events)
5. [Testing and troubleshooting](#tt)

<a name="intro"/>
## Introduction

This guide takes you through the process for integrating SnowPlow's JavaScript tracker `snowplow.js` into your website or web app.

This guide assumes that you are working with the hosted version of `snowplow.js` - you will need to make some adjustments if you are bundling `snowplow.js` into your own site's JavaScript; these adjustments are discussed separately in the [Self-Hosting Guide] [selfhosting]

The exact integration steps required vary depending on whether you choose to use `snowplow.js` in a synchronous or an asynchronous manner; each option is covered separately below.

<a name="async"/>
## Asynchronous integration

### Installing the header script

To use `snowplow.js` in an 'async' manner, first add the following script into your website template's `<head>` section:

```html
<!-- SnowPlow starts plowing -->
<script type="text/javascript">
var _snaq = _snaq || [];

_snaq.push(['setAccount', '{{ACCOUNT}}']);
_snaq.push(['trackPageView']);
_snaq.push(['enableLinkTracking']);

(function() {
var sp = document.createElement('script'); sp.type = 'text/javascript'; sp.async = true; sp.defer = true;
sp.src = ('https:' == document.location.protocol ? 'https' : 'http') + '://snplow.com/sp.js';
var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(sp, s);
})();
 </script>
<!-- SnowPlow stops plowing -->
```

To explain a few things about this code:

* You must update `{{ACCOUNT}}` with your specific account ID provided by the SnowPlow team (which looks something like `d2i847wvqleb11`)
* This code works with both HTTPS (i.e. SSL-secured) and HTTP pages
* The `trackPageView` command logs the page load 

### Integrating event tracking

Having set up the asynchronous tracking above, you can now add SnowPlow's asynchronous event tracking into your website or webapp.

Here is an example of SnowPlow event tracking attached to a simple JavaScript action:

```html
<!-- Website event with SnowPlow tracking -->
<script type="text/javascript">
    function playVideo(){
        alert("Playing a video")
        _snaq.push(['trackEvent', 'Videos', 'Play', 'Fargo', '320x200'])
    }
</script>
```

The four arguments to the event tracking command may be broadly familiar to you from Google Analytics - however there are some small differences, so please refer to the section [Event tracking](#events) below for more information.

Any problems? Please consult the [Testing and troubleshooting](#tt) section at the bottom of this guide.

<a name="sync"/>
## Synchronous integration

### Installing the header script

To use `snowplow.js` in a 'sync' manner, first add the following script into your website template's `<head>` section:

```html
<!-- SnowPlow starts plowing -->
<script type="text/javascript">
var sp-src = ('https:' == document.location.protocol) ? 'https' :'http') + '://snplow.com/sp.js');
document.write(unescape("%3Cscript src'" + sp-src + "' type='text/javascript'%3E%3C/script%3E"));
</script>
<script type="text/javascript">
try {
var snowplowTracker = SnowPlow.getAccount({{ACCOUNT}});
snowplow.trackPageView();
snowplow.enableLinkTracking();
} catch ( err ) {}
</script>
<!-- SnowPlow stops plowing -->

<!-- Piwik --> <script type="text/javascript">
var pkBaseURL = (("https:" == document.location.protocol) ? "https://{$PIWIK_URL}/" : "http://{$PIWIK_URL}/");
document.write(unescape("%3Cscript src='" + pkBaseURL + "piwik.js' type='text/javascript'%3E%3C/script%3E"));
</script><script type="text/javascript">
try {
var piwikTracker = Piwik.getTracker(pkBaseURL + "piwik.php", {$IDSITE});
piwikTracker.trackPageView();
piwikTracker.enableLinkTracking();
} catch( err ) {}
</script>
<!-- End Piwik Code -->
```


### Integrating event tracking

**This section still to write.**

Any problems? Please consult the [Testing and troubleshooting](#tt) section at the bottom of this guide.

<a name="events"/>
## Event tracking

_This section is common to both the synchronous and asynchronous integration approaches._

### Philosophy

The concept of event tracking is at the heart of SnowPlow. In the 'classical' model of web analytics, sensible analyses are agreed in advance, then formalised by being integrated into the site (e.g. by tracking goals and conversion funnels) and finally analysed. SnowPlow views this approach as 'premature analysis', and encourages logging plenty of intent-agnostic events and then figuring out what they mean later.

The Event tracking sections of the synchronous and asynchronous guides covers the technical integration of SnowPlow events; in the rest of this section we provide a practical guide to using event tracking effectively.

### Anatomy of event tracking

The SnowPlow concept of an event has five key attributes:

| **Name**    | **Required?** | **Description**                                                                  |
|------------:|:--------------|:---------------------------------------------------------------------------------|
|  `Category` | Yes           | The name you supply for the group of objects you want to track                   |
|    `Action` | Yes           | A string which defines the type of user interaction for the web object           |
|    `Object` | No            | An optional string which identifies the specific object being actioned           |
|  `Property` | No            | An optional string describing the object or the action performed on it           |
|     `Value` | No            | An optional float to quantify or further describe the user action                |

If you have setup event tracking with Google Analytics, these will seem familiar. Here are the differences between SnowPlow's approach and Google Analytics':

* The SnowPlow `Object` field is the equivalent of `Label` in Google Analytics
* The SnowPlow `Value` field takes a floating point number (e.g. '3.14') while the equivalent field in Google Analytics takes an integer 
* SnowPlow has an additional `Property` field, which takes a string and can be used to further describe the object or the action performed on it
* SnowPlow does not have a boolean field called `Non-interaction`

### Examples

#### Playing a music mix

Here is an (asynchronous) example of tracking a user listening to a music mix:

```javascript
_snaq.push(['_trackEvent', 'Mixes', 'Play', 'MrC/fabric-0503-mix', , '0.0']);
```

The explanation of each argument passed to `'_trackEvent'` is as follows:

| **Argument**          | **Attribute** | **Explanation**                                                         |
|----------------------:|:--------------|:------------------------------------------------------------------------|
| 'Mixes'               | `Category`    | This is a DJ mix on a music site                                        |
| 'Play'                | `Action`      | We are tracking a mix being played                                      | 
| 'MrC/fabric-0503-mix' | `Object`      | This uniquely identifies the mix being played                           |
| `, ,` i.e. not set    | `Property`    | Not required                                                            |
| '0.0'                 | `Value`       | A float specifying how far in seconds into the mix the playback started |

##### Adding a product to basket

Here is a (synchronous) example of tracking a user adding a product to their shopping basket:

```javascript
_trackEvent('Checkout', 'Add', 'ASO01043', 'blue:xxl', '2.0']);
```

The explanation of each argument passed to `_trackEvent()` is as follows:

| **Argument** | **Attribute** | **Explanation**                                                                                         |
|-------------:|:--------------|:--------------------------------------------------------------------------------------------------------|
| 'Checkout'   | `Category`    | This is the checkout flow on an ecommerce site                                                          |
| 'Add'        | `Action`      | The user is adding a product to his or her shopping basket                                              | 
| 'ASO01043'   | `Object`      | A SKU uniquely identifying the product added to basket                                                  |
| 'blue:xxl'   | `Property`    | Describes the product (we're actually compressing two properties into one with a colon in-between them) |
| '2.0'        | `Value`       | A float specifying how many units of the product the user is adding to basket                           |

### Further reading

For further examples and additional background on the concepts around web event tracking, we would encourage you to read Google Analytics's [Event Tracking Guide] [gaeventguide], as there are many similarities between the two approaches. 

<a name="tt"/>
## Testing and troubleshooting

_This section is common to both the synchronous and asynchronous integration approaches._

### Checking snowplow.js is working

You can 'kick the tyres' of `snowplow.js` with the example HTML pages available in:

    snowplow/tracker/examples

Before running these HTML pages, make sure to update the `{{ACCOUNT}}` to your SnowPlow-supplied account ID.

We recommend using Chrome's [Developer Tools] [chromedevtools] or [Firebug] [firebug] for Firefox to check that SnowPlow's JavaScript tracking is working correctly. Here is what Chrome's Network panel looks like after loading the page and clicking each button once:

![networkpane] [networkpane]

Note the three successful (status code 200) `GET`s to `ice.png`. The first of these was triggered by the page load, and then there is one `GET` each for the two user actions (button clicks) that we're tracking. 

If you have any problems getting this to run, please [contact] [contact] the SnowPlow team.

### Confirming SnowPlow is logging the right data

If you are using the SnowPlow-hosted version of SnowPlow, then please [contact] [contact] the SnowPlow team to confirm that your event data is being successfully logged. If you are self-hosting SnowPlow, then consult the relevant section within the Self-Hosting Guide.

[selfhosting]: /snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md
[gaeventguide]: http://code.google.com/apis/analytics/docs/tracking/eventTrackerGuide.html
[chromedevtools]: http://code.google.com/chrome/devtools/docs/overview.html
[firebug]: http://getfirebug.com/
[networkpane]: /snowplow/snowplow/raw/master/docs/images/03_network_pane.png
[contact]: mailto:snowplow@keplarllp.com