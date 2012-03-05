# Integrating snowplow.js

## Introduction

This guide takes you through the process for integration SnowPlow's JavaScript tracker (`snowplow.js`) into your website or web app.

This guide assumes that you are working with the hosted version of `snowplow.js` - you will need to make some adjustments if you are bundling `snowplow.js` into your own site JavaScript; these adjustments are discussed separately in the [Self-Hosting Guide] [selfhosted]

The exact integration steps required vary depending on whether you choose to use `snowplow.js` in a synchronous or an asynchronous manner; each option is covered separately below.

## Asynchronous integration

### The header script

To use `snowplow.js` in an 'async' manner, first add the following script into your website template's `<head>` section:

```html
<!-- SnowPlow starts plowing -->
<script type="text/javascript">
var _snaq = _snaq || [];

_snaq.push(['setTrackerUrl', 'http://{{CLIENT}}.snplow.com/pxl.png']);
_snaq.push(['trackPageView']);

(function() {
var sp = document.createElement('script'); sp.type = 'text/javascript'; sp.async = true; sp.defer = true;
sp.src = ('https:' == document.location.protocol ? 'https' : 'http') + '://js.snplow.com/sp.js';
var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(sp, s);
})();
 </script>
<!-- SnowPlow stops plowing -->
```

To explain a few things about this code:

* You must update `{{CLIENT}}` to be your specific sub-domain provided by the SnowPlow team
* This code work with both `https` (SSL-secured) and `http` pages
* The `trackPageView` command logs the page load 

### Event tracking

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

The four arguments to the event tracking command may be familiar to you from the Google Analytics API - however there are some small differences, so please refer to the section **Event tracking** below for more information.

Any problems? Please consult the **Testing and troubleshooting** section at the bottom of this guide.

## Synchronous integration

### The header script

**This section still to write.**

### Event tracking

**This section still to write.**

## Focus on event tracking

_This section is common to both the synchronous and asynchronous integration approaches._

### Philosophy

The concept of event tracking is at the heart of SnowPlow. In the 'classical' model of web analytics, the sensible analyses are agreed in advance, then formalised by being integrated into the site (e.g. by tracking goals and conversion funnels) and finally analysed. SnowPlow views this approach as 'premature analysis', and encourages logging lots of intent-agnostic events and then figuring out what they mean later.

The **Event tracking** sections of the synchronous and asynchronous guides covers the technical integration of events; in the rest of this section we provide more of a practical guide to using event tracking effectively. 

### Anatomy of event tracking

The SnowPlow concept of an event has five key attributes:

| **Name**  | **Required?** | **Description**                                                                  |
|----------:|:--------------|:---------------------------------------------------------------------------------|
|  Category | Yes           | The name you supply for the group of objects you want to track                   |
|    Action | Yes           | A string which defines the type of user interaction for the web object           |
|    Object | No            | An optional string which identifies the specific object being actioned           |
|  Property | No            | An optional string describing the object or the action performed on it           |
|     Value | No            | An optional float to quantify or further describe the user action                |

If you have setup event tracking with Google Analytics, these will seem very familiar. Here are the differences:

* The SnowPlow `Object` field is the equivalent of `Label` in Google Analytics
* The SnowPlow `Value` field takes a floating point number (e.g. '3.14') whereas Google Analytics' equivalent field takes an integer 
* SnowPlow has an additional `Property` field, which takes a string and can be used to further describe the object or the action performed on it
* SnowPlow does not have a boolean field called `Non-interaction`

### Examples

Here is an (asynchronous) example of tracking a user listening to a music mix:

```javascript
_snaq.push(['_trackEvent', 'Mixes', 'Play', 'ToddlaT/radio-1-essential-mix', , '0.0']);
```

**TODO: confirm syntax of this**

where:

* 'Mixes' is the `Category` of object: mixes on a music site
* 'Play' is the `Action` we are tracking
* 'ToddlaT/radio-1-essential-mix' uniquely identifies the `Object`
* `, ,` means we have no `Property` attribute to store
* '0.0' is the float `Value` specifying where in seconds within the mix the `Action` started 

Here is a (synchronous) example of tracking an add-to-basket event:

```javascript
_trackEvent('Cart', 'Add', 'ASO01043', 'blue:xxl', '2.0']);
```

where:

* 'Cart' is the `Category` of object: checkout flow on an ecommerce site
* 'Add' is the `Action`, and signifies adding to basket
* 'ASO01043' is the `Object`: a SKU uniquely identifying the product added to basket
* 'blue:xxl' is the `Property` attribute for the product (we're actually compressing two properties into one with a colon in-between them)
* '2.0' is the float `Value` specifying how many units of the product the user is adding to basket

**TODO: confirm syntax of this**

### Further reading

For further examples and additional background on the concepts around web event tracking, we would encourage you to read Google Analytics's [Event Tracking Guide] [gaeventguide], as there are many similarities between the two approaches. 

## Testing and troubleshooting

_This section is common to both the synchronous and asynchronous integration approaches._

**This section still to write.**

[selfhosted]: http://todo
[gaeventguide]: http://code.google.com/apis/analytics/docs/tracking/eventTrackerGuide.html